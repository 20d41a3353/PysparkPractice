from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, coalesce, when, collect_list, struct
import traceback
import sys
from common.createSpark import create_spark_session
from common.fileUtil import get_hdfs_base
from datetime import timedelta
import common.constants as constants
from common.dateUtil import get_etl_date
from common.dateUtil import get_etl_date
from common.fileUtil import paths_for_read
from common.processRun import get_process_run_id
from common.log_etl_modified import log_etl
from common.read_write_oracle import read_oracle, write_oracle
from common.fileUtil import tables_to_read

def main(args):
    HDFS_BASE = get_hdfs_base()
    spark = create_spark_session("CNTR_AND_SUSPENSE_TRACKER", HDFS_BASE)

    try:
        oracle_tables = tables_to_read("glif")
        
        today = get_etl_date(spark)
        run_date = today.strftime("%d-%m-%Y")
        posting_date_str = run_date
        yesterday = (today - timedelta(days=1)).strftime("%d-%m-%Y")
        print(f"RUN DATE      : {run_date}")
        print(f"YESTERDAY DATE: {yesterday}")
        hdfs_paths = paths_for_read("glif",today,posting_date_str)
        BALANCE = oracle_tables["BALANCE"]
        GL_DATALAKE_PATH = hdfs_paths["GL_DATALAKE_PATH"]

        PROCESS_ID = getattr(constants,"DIFFERENCE_PIPELINE_ID",3) 
 
        process_run_id = ( 
            sys.argv[1] 
            if len(sys.argv) > 1 
            else get_process_run_id( 
                spark, 
                PROCESS_ID, 
                run_date 
            ) 
        ) 
        print(f"Process RunId : {process_run_id}") 

        try:

            # ====================================================== 
            # ======================================================
            # PART 1 : CNTR SUMMARY
            # ======================================================
            # ======================================================
            

            print("====================================")
            print("STARTING CNTR SUMMARY")
            print("====================================")
            log_etl(spark,process_run_id,"cntr_summary_started",1,"Generating CNTR Summary")
            query = f"""
            (
                SELECT
                    CGL,
                    CURRENCY,
                    COUNT(1) AS COUNT
                FROM {BALANCE}
                WHERE CGL IN (
                    '1260505001',
                    '1260505003',
                    '1111111111'
                )
                AND BALANCE_DATE = TO_DATE(
                    '{run_date}',
                    'DD-MM-YYYY'
                )
                AND BALANCE <> 0  
                GROUP BY CGL, CURRENCY
            ) T
            """
            try:
                path_config = paths_for_read("glif", today, posting_date_str)
                gl_delta_path = path_config["GL_DATALAKE_PATH"]

                result_df = spark.read.format("delta").load(gl_delta_path) \
                    .filter(F.col("BALANCE_DATE") == F.lit(posting_date_str)) \
                    .withColumns({ 
                        "BRANCH_CODE": F.substring(F.col("glcc"), 1, 5),
                        "CURRENCY": F.substring(F.col("glcc"), 6, 3),
                        "CGL": F.substring(F.col("glcc"), 9, 10),
                        "BALANCE": F.col("CLOSING_BALANCE")
                    }) \
                    .select("CGL", "CURRENCY", "BALANCE")
                # result_df.show(20, False)
                
                target_cgls = ['1260505001', '1260505003', '1111111111']

                filtered_df = result_df \
                    .filter(F.col("CGL").isin(target_cgls)) \
                    .filter(F.col("BALANCE") != 0) \
                    .groupBy("CGL", "CURRENCY") \
                    .agg(F.count("*").alias("count"))

                summary_df = filtered_df.select(
                    F.collect_list(
                        F.struct(
                            "CGL",
                            "CURRENCY",
                            "count"
                        )
                    ).alias("summary")
                )
                summary_df.printSchema()
                # summary_df.show(truncate=False)
                
                final_df = summary_df.select(
                    lit("CNTR Summary").alias(
                        "Summary Name"
                    ),
                    lit(run_date).alias("date"),
                    col("summary")
                )
                # final_df.show(100, False)

                final_json = final_df.selectExpr(
                    "to_json(struct(*)) AS value"
                ).first()["value"]
                    
                print(final_json)
                print("============================ FINAL JSON ===================================")

                log_etl(spark,process_run_id,"cntr_summary_calculated",2,"CNTR Summary Successfully Calculated",0,final_json)

                log_etl(spark,process_run_id,"cntr_summary_completed",2,"CNTR Summary generation completed")

                print("====================================")
                print("CNTR SUMMARY COMPLETED")
                print("====================================")

            except Exception as e:
                print(f"Error {e} while fetching data from GL DATALAKE")
                try:
                    result_df = read_oracle(spark, query)
                    summary_df = result_df.select(

                        collect_list(
                             struct(
                                "cgl",
                                "currency",
                                "count"
                            )
                        ).alias("summary")
                    )
                    final_df = summary_df.select(

                        lit("CNTR Summary").alias(
                            "Summary Name"
                        ),
                        lit(run_date).alias("date"),
                        col("summary")
                    )
                    # final_df.show(100, False)

                    final_json = final_df.selectExpr(
                        "to_json(struct(*)) AS value"
                    ).first()["value"]
                    print(final_json)
                    print("============================ FINAL JSON ===================================")

                    log_etl(spark,process_run_id,"cntr_summary_calculated",2,"CNTR Summary Successfully Calculated",0,final_json)

                    log_etl(spark,process_run_id,"cntr_summary_completed",2,"CNTR Summary generation completed")
                    print("====================================")
                    print("CNTR SUMMARY COMPLETED")
                    print("====================================")
                except Exception as e:
                    print(f"Error {e} while fetching data from {BALANCE} Table")
                    log_etl(spark, process_run_id, "reading_closing_balance", 3, "Error reading from Delta")
                    spark.stop()
                    exit(1)

        except Exception as e:
            print("ERROR IN CNTR SUMMARY")
            traceback.print_exc()

            log_etl(spark,process_run_id,"cntr_summary_failed",3,f"Error while generating CNTR Summary : {str(e)}")
    

        # ======================================================
        # ======================================================
        # PART 2 : SUSPENSE TRACKER STARTED
        # ======================================================
        # ======================================================
        try:

            print("====================================")
            print("STARTING SUSPENSE_TRACKER")
            print("====================================")

            log_etl(spark,process_run_id,"suspense_tracker_started",1,"Suspense Tracker started")

            path_config = paths_for_read("glif", today, posting_date_str)
            gl_delta_path = path_config["GL_DATALAKE_PATH"]

            target_cgls = [
                '1111111111', '1111111112', '1260505001', '2148505001',
                '1148505004', '1148505005', '1148505006', '1148505007',
                '1148505010', '1148505012', '1148505014', '1148505020',
                '2148505004', '2148505006', '2148505007', '2148505010',
                '2148505011', '2148505014', '2148505016', '2148505018',
                '2148505019', '2148505021', '2148505022', '2148505024',
                '2148505033', '2148505035', '1260505002', '1260505003'
            ]
            cgl_list =  ", ".join([f"'{x}'" for x in target_cgls])
            
            # ======================================================
            # READING TODAY + YESTERDAY 
            # ======================================================

            try:

                balance_df = (
                    spark.read.format("delta")   
                    .load(gl_delta_path)
                    .filter(
                        F.col("BALANCE_DATE").isin(
                            posting_date_str,
                            yesterday
                        )
                    )
                    .withColumns({
                        "BRANCH_CODE": F.substring(F.col("glcc"), 1, 5),
                        "CURRENCY": F.substring(F.col("glcc"), 6, 3),
                        "CGL": F.substring(F.col("glcc"), 9, 10)
                    })
                    .filter(F.col("CGL").isin(target_cgls))
                )

                today_df = (
                    balance_df
                    .filter(
                        F.col("BALANCE_DATE") == run_date
                    )
                    .select(
                        "BRANCH_CODE",
                        "CURRENCY",
                        "CGL",
                        F.col("CLOSING_BALANCE").alias("T_BAL"),
                        F.col("INR_BALANCE").alias("T_INR_BAL")
                    )
                )

                yest_df = (
                    balance_df
                    .filter(
                        F.col("BALANCE_DATE") == yesterday
                    )
                    .select(
                        "BRANCH_CODE",
                        "CURRENCY",
                        "CGL",
                        F.col("CLOSING_BALANCE").alias("Y_BAL"),
                        F.col("INR_BALANCE").alias("Y_INR_BAL")
                    )
                )

            except Exception as e:

                print(f"Error {e} while fetching data from datalake")

                balance_df = read_oracle(
                    spark,
                    f"""
                    (
                        SELECT
                            BALANCE_DATE,
                            BRANCH_CODE,
                            CURRENCY,
                            CGL,
                            BALANCE,
                            INR_BALANCE
                        FROM {BALANCE}
                        WHERE CGL IN ({cgl_list})
                        AND BALANCE_DATE BETWEEN
                            TO_DATE('{yesterday}','DD-MM-YYYY')
                        AND
                            TO_DATE('{run_date}','DD-MM-YYYY')
                    ) T
                    """
                )

                today_df = (
                    balance_df
                    .filter(
                        F.date_format(
                            F.col("BALANCE_DATE"),
                            "dd-MM-yyyy"
                        ) == run_date
                    )
                    .select(
                        "BRANCH_CODE",
                        "CURRENCY",
                        "CGL",
                        F.col("BALANCE").alias("T_BAL"),
                        F.col("INR_BALANCE").alias("T_INR_BAL")
                    )
                )

                yest_df = (
                    balance_df
                    .filter(
                        F.date_format(
                            F.col("BALANCE_DATE"),
                            "dd-MM-yyyy"
                        ) == yesterday
                    )
                    .select(
                        "BRANCH_CODE",
                        "CURRENCY",
                        "CGL",
                        F.col("BALANCE").alias("Y_BAL"),
                        F.col("INR_BALANCE").alias("Y_INR_BAL")
                    )
                )
            # ======================================================
            # EXISTING DF
            # ======================================================
            existing_df = read_oracle(
                spark,
                f"""
                (
                    SELECT
                        BRANCH_CODE,
                        CURRENCY,
                        CGL,
                        FIRST_ERROR_DATE
                    FROM SUSPENSE_TRACKER
                    WHERE BALANCE_DATE = TO_DATE(
                    '{yesterday}',
                    'DD-MM-YYYY')
                ) E
                """   
            )
            # ======================================================
            # JOINING TODAY AND YESTERDAY
            # ======================================================
            joined_df = today_df.alias("t").join(
                yest_df.alias("y"),
                on=[
                    "BRANCH_CODE",
                    "CURRENCY",
                    "CGL"
                ],
                how="full_outer"
            )

            # ======================================================
            # TYPE LOGIC
            # ======================================================
            result_df = joined_df.withColumn(
                "TYPE",
                when(
                    (coalesce(col("Y_BAL"), lit(0)  ) == 0) &
                    (coalesce(col("T_BAL"), lit(0)) != 0),
                    lit("NEW_ENTRY")
                )
                .when(

                    (coalesce(col("Y_BAL"), lit(0)) != 0) &
                    (coalesce(col("T_BAL"), lit(0)) != 0) & 
                    (
                        (col("Y_BAL") != col("T_BAL")) |
                        (col("Y_INR_BAL") != col("T_INR_BAL"))
                    ),
                    lit("DIFFERENCE")
                )
                .when(
                    (coalesce(col("Y_BAL"), lit(0)) != 0) &
                    (coalesce(col("T_BAL"), lit(0)) == 0),
                    lit("CLOSED")
                )
                .when(
                    (coalesce(col("Y_BAL"), lit(0)) != 0) &
                    (coalesce(col("T_BAL"), lit(0)) != 0) &
                    (col("Y_BAL")==col("T_BAL")),
                    lit("ZERO_DIFFERENCE")
                )
                .otherwise(
                    lit("IGNORE")
                )
            )

            # ======================================================
            # JOINING EXISTING FIRST ERROR DATE
            # ======================================================
            result_df = result_df.join(
                existing_df,
                on=[
                    "BRANCH_CODE",
                    "CURRENCY",
                    "CGL"
                ],
                how="left"
            )
            # ======================================================
            # FIRST ERROR DATE LOGIC
            # ======================================================
            result_df = result_df.withColumn(
                "FINAL_FIRST_ERROR_DATE",
                when(col("TYPE") == "NEW_ENTRY",lit(today))
                .when(col("TYPE").isin("DIFFERENCE", "ZERO_DIFFERENCE", "CLOSED"),
                    coalesce(col("FIRST_ERROR_DATE"), lit(today))
                )
            )
            # ======================================================
            # REMOVING IGNORE
            # ======================================================
            result_df = result_df.filter(col("TYPE") != "IGNORE")
            # ======================================================
            # FINAL DF
            # ======================================================
            final_df = result_df.select(
                lit(today).alias("BALANCE_DATE"),
                col("BRANCH_CODE"),
                col("CURRENCY"),
                col("CGL"),
                coalesce(col("T_BAL"),lit(0)).alias("BALANCE"),
                coalesce(col("T_INR_BAL"),lit(0)).alias("INR_BALANCE"),
                col("TYPE"),
                col("FINAL_FIRST_ERROR_DATE").alias("FIRST_ERROR_DATE")
            )
            # final_df.show(200, False)

            # ======================================================
            # WRITING TO ORACLE
            # ======================================================
            write_oracle(final_df,"SUSPENSE_TRACKER")

            log_etl(spark,process_run_id,"suspense_tracker_completed",2,"Suspense Tracker completed successfully")
            print("====================================")
            print("SUSPENSE_TRACKER COMPLETED")
            print("====================================")
            print("FULL JOB COMPLETED")
        except Exception as e:
            print("ERROR IN SUSPENSE_TRACKER")
            traceback.print_exc()
            log_etl(spark,process_run_id,"suspense_tracker_failed",3,f"Error in Suspense Tracker : {str(e)}")
            spark.stop()
            
    except Exception as e:
        print("ERROR IN CNTR_AND_SUSPENSE_TRACKER ")


if __name__ == "__main__":
    main(sys.argv)
