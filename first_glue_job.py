import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, date_format
from awsglue.dynamicframe import DynamicFrame
from constants import Constants

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


print("================ GLUE JOB START ====================")

ride_input_cols = [
    "vendorid",
    "payment_type",
    "ratecodeid",
    "pulocationid",
    "dolocationid",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "airport_fee",
    "congestion_surcharge",
    "total_amount",
    "improvement_surcharge",
    "tolls_amount",
    "tip_amount",
    "mta_tax",
    "extra",
    "fare_amount",
    "trip_distance",
    "passenger_count"
]

ride_out_cols = [
    "vendor_id",
    "pickup_datetime_id",
    "dropoff_datetime_id",
    "pickup_location_id",
    "dropoff_location_id",
    "rate_code_id",
    "payment_type_id",
    "airport_fee",
    "congestion_surcharge",
    "total_amount",
    "improvement_surcharge",
    "tolls_amount",
    "tip_amount",
    "mta_tax",
    "extra",
    "fare_amount",
    "trip_distance",
    "passenger_count",
]

rides_preaction_query = """
    CREATE TABLE IF NOT EXISTS public.rides_fact (
        vendor_id INTEGER,
        pickup_datetime_id INTEGER,
        dropoff_datetime_id INTEGER,
        pickup_location_id INTEGER,
        dropoff_location_id INTEGER,
        rate_code_id INTEGER,
        payment_type_id INTEGER,
        airport_fee REAL,
        congestion_surcharge REAL,
        total_amount REAL,
        improvement_surcharge REAL,
        tolls_amount REAL, 
        tip_amount REAL,
        mta_tax REAL,
        extra REAL,
        fare_amount REAL,
        trip_distance REAL,
        passenger_count INTEGER,
        FOREIGN KEY (pickup_datetime_id) REFERENCES datetime_dim (datetime_id),
        FOREIGN KEY (dropoff_datetime_id) REFERENCES datetime_dim (datetime_id),
        FOREIGN KEY (pickup_location_id) REFERENCES location_dim (location_id),
        FOREIGN KEY (dropoff_location_id) REFERENCES location_dim (location_id),
        FOREIGN KEY (rate_code_id) REFERENCES rate_code_dim (rate_code_id),
        FOREIGN KEY (payment_type_id) REFERENCES payment_type_dim (payment_type_id)
    );"""

datetime_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="yellow-taxi",
    table_name="dim_datetime",
    transformation_ctx="datetime_dyf"
)
datetime_dyf.show(5)
print("Schema for the locations DynamicFrame: \n", datetime_dyf.printSchema())

# rides_dyf = glueContext.create_dynamic_frame.from_catalog(
#     database="yellow-taxi",
#     table_name="fact_rides",
#     transformation_ctx="rides_dyf",
#     push_down_predicate="date='2023-10-02'"
# )
# rides_dyf.show(5)
# print("Schema for the rides DynamicFrame: \n", rides_dyf.printSchema())

# https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-csv-home.html
rides_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://yellow-taxi-pipeline-data/fact_data/rides/date=2023-10-03/"], "recurse": True},
    format="csv",
    format_options={
        "withHeader": True,
    },
    transformation_ctx="rides_dyf",
)
# rides_dyf.show(5)
# print("Schema for the Rides DynamicFrame: \n", rides_dyf.printSchema())

payment_type_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="yellow-taxi",
    table_name="dim_payment_type",
    transformation_ctx="payment_type_dyf"
)


# print("Schema for the Payment Type DynamicFrame: \n", payment_type_dyf.printSchema())
# print(payment_type_dyf.show())

rate_code_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="yellow-taxi",
    table_name="dim_rate_code",
    transformation_ctx="rate_code_dyf"
)
# print("Schema for the Rate Code DynamicFrame: \n", rate_code_dyf.printSchema())


datetime_spark_df = datetime_dyf.toDF()
rides_spark_df = rides_dyf.toDF()
payment_spark_df = payment_type_dyf.toDF()
rate_code_spark_df = rate_code_dyf.toDF()

# print("============== NULL VALUES =================")
# rides_spark_df.filter(col("pulocationid").isNull()).show()

# datetime_spark_df.show(5)
# rides_spark_df.show(5)

rides_spark_df = rides_spark_df.withColumn(
    "tpep_pickup_datetime", date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:00:00"))
rides_spark_df = rides_spark_df.withColumn(
    "tpep_dropoff_datetime", date_format(col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:00:00"))


# Alias the DataFrames
rides_df = rides_spark_df.alias("rides")
datetime_df = datetime_spark_df.alias("datetime")

# Join the DataFrames and specify qualified column names
rides_datetime_join_df = rides_df.select(ride_input_cols).join(
    datetime_df.select(["datetime_id", "datetime"]), 
    rides_df["tpep_pickup_datetime"] == datetime_df["datetime"]
)
rides_datetime_join_df = rides_datetime_join_df.withColumnRenamed("datetime_id", "pickup_datetime_id")
rides_datetime_join_df = rides_datetime_join_df.drop("datetime")

# rides_datetime_join_df.show(5)


# Join the DataFrames and specify qualified column names
rides_datetime_join_df = rides_datetime_join_df.join(
    datetime_df.select(["datetime_id", "datetime"]), 
    rides_datetime_join_df["tpep_dropoff_datetime"] == datetime_df["datetime"]
)
rides_datetime_join_df = rides_datetime_join_df.withColumnRenamed("datetime_id", "dropoff_datetime_id")
rides_datetime_join_df = rides_datetime_join_df.drop("datetime")

# rides_datetime_join_df.show(5)

# cast rate_code and rename it
rides_datetime_join_df = rides_datetime_join_df.fillna(value="1.0", subset=['ratecodeid'])
rides_datetime_join_df = rides_datetime_join_df.fillna(value="0.0", subset=['airport_fee'])
rides_datetime_join_df = rides_datetime_join_df.fillna(value="0.0", subset=['congestion_surcharge'])
rides_datetime_join_df = rides_datetime_join_df.fillna(value="1", subset=['passenger_count'])

rides_datetime_join_df = (
    rides_datetime_join_df.withColumnRenamed("ratecodeid", "rate_code_id")
    .withColumn("rate_code_id", col("rate_code_id").cast("int")))
rides_datetime_join_df = rides_datetime_join_df.filter(col('rate_code_id') != 99)

rides_datetime_join_df = (
    rides_datetime_join_df.withColumn("passenger_count", col("passenger_count").cast("int")))

rides_datetime_join_df = rides_datetime_join_df.withColumnRenamed("payment_type", "payment_type_id")
rides_datetime_join_df = rides_datetime_join_df.withColumnRenamed("vendorid", "vendor_id")
rides_datetime_join_df = rides_datetime_join_df.withColumnRenamed("pulocationid", "pickup_location_id")
rides_datetime_join_df = rides_datetime_join_df.withColumnRenamed("dolocationid", "dropoff_location_id")

# rides_datetime_join_df.show(5)

# Show the result
final_spark_df = rides_datetime_join_df.select(ride_out_cols)
# final_spark_df.show(5)
# final_spark_df.printSchema()

final_dyf = DynamicFrame.fromDF(final_spark_df, glueContext, "final_dyf")
# final_dyf.printSchema()
# final_dyf.show(5)

final_dyf_mapped = final_dyf.apply_mapping(
    [
        ("vendor_id", "string", "vendor_id", "integer"),
        ("pickup_datetime_id", "long", "pickup_datetime_id", "integer"),
        ("dropoff_datetime_id", "long", "dropoff_datetime_id", "integer"),
        ("pickup_location_id", "string", "pickup_location_id", "integer"),
        ("dropoff_location_id", "string", "dropoff_location_id", "integer"),
        ("rate_code_id", "int", "rate_code_id", "integer"),
        ("payment_type_id", "string", "payment_type_id", "integer"),
        ("airport_fee", "string", "airport_fee", "float"),
        ("congestion_surcharge", "string", "congestion_surcharge", "float"),
        ("total_amount", "string", "total_amount", "float"),
        ("improvement_surcharge", "string", "improvement_surcharge", "float"),
        ("tolls_amount", "string", "tolls_amount", "float"),
        ("tip_amount", "string", "tip_amount", "float"),
        ("mta_tax", "string", "mta_tax", "float"),
        ("extra", "string", "extra", "float"),
        ("fare_amount", "string", "fare_amount", "float"),
        ("trip_distance", "string", "trip_distance", "float"),
        ("passenger_count", "int", "passenger_count", "integer"),
    ],
    transformation_ctx="final_dyf_mapped",
)
# final_dyf_mapped.printSchema()
final_dyf_mapped.show(5)

# # def is_null(record):
# #     # Assuming column_name is the name of the column you want to check for null values
# #     return record["pickup_location_id"] is None

# # final_dyf_mapped.filter(is_null).show(5)

# Script generated for node Amazon Redshift
final_rides_dyf = glueContext.write_dynamic_frame.from_options(
    frame=final_dyf_mapped,
    connection_type="redshift",
    connection_options = {
        "url": "jdbc:redshift://my-redshift-cluster.cezy0qn980ta.us-east-1.redshift.amazonaws.com:5439/dev",
        "dbtable": "public.rides_fact",
        "user": Constants.REDSHIFT_DB_USER,
        "password": Constants.REDSHIFT_DB_PASSWORD,
        "redshiftTmpDir": "s3://smartcity-queries/temp/",
        "aws_iam_role": Constants.REDSHIFT_CLUSTER_ROLE,
        "preactions": rides_preaction_query,
    },
    transformation_ctx="final_rides_dyf",
)

# print("================ GLUE JOB END ====================")

