from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col, array_contains

spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkExamples') \
                    .getOrCreate()

df = spark.read.csv('/Users/MaltsevEA/Documents/Dev/Spark/data/simple-zipcodes.csv')
df.printSchema()
# Output
# root
#  |-- _c0: string (nullable = true)
#  |-- _c1: string (nullable = true)
#  |-- _c2: string (nullable = true)
#  |-- _c3: string (nullable = true)
#  |-- _c4: string (nullable = true)

df2 = spark.read.option("header", True) \
     .csv("/Users/MaltsevEA/Documents/Dev/Spark/data/simple-zipcodes.csv")

df2.printSchema()
# Output
# root
#  |-- RecordNumber: string (nullable = true)
#  |-- Country: string (nullable = true)
#  |-- City: string (nullable = true)
#  |-- Zipcode: string (nullable = true)
#  |-- State: string (nullable = true)

df3 = spark.read.options(header='True', delimiter=',') \
  .csv("/Users/MaltsevEA/Documents/Dev/Spark/data/simple-zipcodes.csv")
df3.printSchema()


schema = StructType() \
      .add("RecordNumber",IntegerType(),True) \
      .add("Country",StringType(),True) \
      .add("City",StringType(),True) \
      .add("Zipcode",IntegerType(),True) \
      .add("State",StringType(),True)

df_with_schema = spark.read.format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load("/Users/MaltsevEA/Documents/Dev/Spark/data/simple-zipcodes.csv")
df_with_schema.printSchema()

df2.write.option("header",True) \
 .csv("/Users/MaltsevEA/Documents/Dev/Spark/Temp/simple-zipcodes.csv")