from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, DoubleType

# ---------------------------------------------------------
# 1. Initialize Spark session with Hive support
# ---------------------------------------------------------
spark = SparkSession.builder \
    .appName("TravelInsuranceTransform") \
    .config("hive.metastore.uris", "thrift://18.134.163.221:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# ---------------------------------------------------------
# 2. Select the Hive database (RAW layer)
# ---------------------------------------------------------
spark.sql("USE maan_db")

# ---------------------------------------------------------
# 3. Read data from the raw Hive table
# ---------------------------------------------------------
df = spark.table("maan_travel_insurance_raw")

# ---------------------------------------------------------
# 4. Apply 10 transformations
# ---------------------------------------------------------
df2 = (
    df
    .toDF(*[c.lower() for c in df.columns])  #Convert all column names to lowercase
    .withColumn("net_sales", F.col("net_sales").cast(DoubleType()))  #Cast columns to proper types
    .withColumn("commission_value", F.col("commission_value").cast(DoubleType()))
    .withColumn("duration", F.col("duration").cast(IntegerType()))
    .withColumn("age", F.col("age").cast(IntegerType()))
    .filter((F.col("duration") > 0) & (F.col("age") > 0))  #Filter invalid values
    .na.drop(subset=["net_sales"])                         #Drop nulls in net_sales
    .withColumn("profit", F.col("net_sales") - F.coalesce(F.col("commission_value"), F.lit(0)))  #Calculate profit
    .withColumn("high_value_sale", F.when(F.col("net_sales") > 1000, 1).otherwise(0))             #Flag high-value sales
    .withColumn("age_group",
                F.when(F.col("age") < 25, "young")
                 .when(F.col("age") < 60, "adult")
                 .otherwise("senior"))                      #Categorize by age group
    .withColumn("destination_short", F.split(F.col("destination"), ' ').getItem(0))               #Extract first word of destination
    .withColumn("product_name_norm", F.trim(F.lower(F.col("product_name"))))                      #Normalize product name
    .withColumn("ingested_ts", F.current_timestamp())                                            #Add timestamp for lineage
)
print(df2.show())

# ---------------------------------------------------------
# 5. Write the curated (transformed) data back to Hive
# ---------------------------------------------------------
#df2.write.mode("overwrite").saveAsTable("maan_db.maan_travel_insurance_curated")

# ---------------------------------------------------------
# 6. Stop Spark session
# ---------------------------------------------------------
spark.stop()

