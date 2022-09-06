# Create SparkSession from builder
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkExamples') \
                    .getOrCreate()


# Create DataFrame
df = spark.createDataFrame(
    [("Scala", 25000), ("Spark", 35000), ("PHP", 21000)])

df.createOrReplaceTempView("sample_table")
df2 = spark.sql("SELECT _1,_2 FROM sample_table")
# df2.show()
#
# # Create Hive table & query it.
# spark.table("sample_table").write.saveAsTable("sample_hive_table")
# df3 = spark.sql("SELECT _1,_2 FROM sample_hive_table")
#
# Get metadata from the Catalog
# List databases
dbs = spark.catalog.listDatabases()
print(dbs)
# Output
#[Database(name='default', description='default database', locationUri='file:/Users/MaltsevEA/Documents/Dev/Spark/source/spark-warehouse')]

# List Tables
tbls = spark.catalog.listTables()
print(tbls)
#Output
#[Table(name='sample_hive_table', database='default', description=None, tableType='MANAGED', isTemporary=False), Table(name='sample_table', database=None, description=None, tableType='TEMPORARY', isTemporary=True)]
