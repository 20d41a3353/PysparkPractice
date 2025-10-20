from pyspark.sql import SparkSession , functions as F

import sys
import os

from pyspark.sql.functions import split, col, expr, trim, when
from pyspark.sql.types import DecimalType

# Set the environment for PySpark and Java
python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['JAVA_HOME'] = r'C:\Users\ABHI\.jdks\ms-17.0.16'
print("creating spark session")
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("pyspark") \
    .master("local[*]") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()

# Read text file from the RAW GitHub URL
url = "data.txt"
result = spark.read.text(url)

result =result.withColumn("Id", expr("subString(value,1,9)"))\
    .withColumn("Amount",expr("subString(value,14,10)"))\
    .withColumn("valueType", expr("subString(value,25)")).drop("value")
print("print the dataframe")
# Show first 20 lines without truncation
result.show(20, False)


# result = result.withColumn("Amount",trim(result["Amount"]).cast(DecimalType(24,2)))
#
# result1 = result.withColumn("Amount",when (result["valueType"]=="+" ,(result["Amount"]/100)))\
# .when (result["valueType"]=="-" ,(-1 * result["Amount"]/100)))\
# .otherwise(0)
result = result.withColumn(
    "Amount",
    when(col("valueType") == "+",  trim(col("Amount")).cast(DecimalType(24, 2)) / 100)
    .when(col("valueType") == "-", -1 * trim(col("Amount")).cast(DecimalType(24, 2)) / 100)
    .otherwise(0)
)

result.show(20,False)

result = result.groupBy("Id")\
    .agg(F.sum("Amount").alias("Amount"))\
    .select("Id","Amount")

result.show(20,False)

