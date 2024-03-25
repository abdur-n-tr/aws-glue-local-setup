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

# rides_df = glueContext.create_dynamic_frame.from_options(
#     connection_type="s3",
#     connection_options={"paths": ["s3://yellow-taxi-pipeline-data/fact_data/rides/date=2023-10-03/"], "recurse": True},
#     format="csv",
#     format_options={
#         "withHeader": True,
#     },
#     transformation_ctx="rides_df",
# )


# Script generated for node rides_s3
# rides_s3_dyf = glueContext.create_dynamic_frame.from_catalog(
#     database="yellow-taxi", 
#     table_name="fact_rides", 
#     transformation_ctx="rides_s3_dyf")

# rides_s3_dyf.show(5)

# # Script generated for node datetime_s3
# datetime_s3_dyf = glueContext.create_dynamic_frame.from_catalog(
#     database="yellow-taxi", 
#     table_name="dim_datetime", 
#     transformation_ctx="datetime_s3_dyf")

# # Script generated for node rides_datetime_format
# rides_datetime_format_dyf = rides_s3_dyf.gs_format_timestamp(
#     colName="tpep_pickup_datetime", 
#     dateFormat="'yyyy-MM-dd HH:00:00'")

# # Script generated for node rides_datetime_join
# rides_datetime_join_dyf = rides_datetime_format_dyf.join(
#     frame2=datetime_s3_dyf,
#     keys1=["tpep_pickup_datetime"], 
#     keys2=["datetime"], 
#     transformation_ctx="rides_datetime_join_dyf"
# )

# print(rides_datetime_join_dyf.printSchema())
# print(rides_datetime_join_dyf.show(5))


# location_mapped = location_df.apply_mapping(
#     [
#         ("LocationID", "string", "location_id", "short"),
#         ("Borough", "string", "borough", "string"),
#         ("Zone", "string", "zone", "string"),
#         ("service_zone", "string", "service_zone", "string"),
#     ],
#     transformation_ctx="location_mapped",
# )
# print(location_mapped.printSchema())

# Script generated for node Amazon Redshift
# location_redshift_df = glueContext.write_dynamic_frame.from_options(
#     frame=location_mapped,
#     connection_type="redshift",
#     connection_options = {
#         "url": "jdbc:redshift://my-redshift-cluster.cezy0qn980ta.us-east-1.redshift.amazonaws.com:5439/dev",
#         "dbtable": "public.location_dim",
#         "user": Constants.REDSHIFT_DB_USER,
#         "password": Constants.REDSHIFT_DB_PASSWORD,
#         "redshiftTmpDir": "s3://smartcity-queries/temp/",
#         "aws_iam_role": Constants.REDSHIFT_CLUSTER_ROLE,
#         "preactions": "CREATE TABLE IF NOT EXISTS public.location_dim (location_id SMALLINT, borough VARCHAR(20), zone VARCHAR(50), service_zone VARCHAR(15));",
#     },
#     transformation_ctx="location_redshift_df",
# )


# my_conn_options = {  
#     "url": "jdbc:redshift://my-redshift-cluster.cezy0qn980ta.us-east-1.redshift.amazonaws.com:5439/dev",
#     "dbtable": "public.location_dim",
#     "user": "awsuser",
#     "password": "Temp1234",
#     "redshiftTmpDir": "s3://smartcity-queries/temp/",
#     "aws_iam_role": "arn:aws:iam::471112663332:role/role-s3-to-redshift-vice-versa"
# }

# redshift_df = glueContext.create_dynamic_frame.from_options("redshift", my_conn_options)
# print("=========== redshift_df ================")
# print(redshift_df.printSchema())
# print("===========================")


# df = location_df.toDF()
# df_pd = df.toPandas()

# print("================ GLUE JOB START ====================")
# print(df_pd)
# print('test script has successfully ran')
# print("================ GLUE JOB END ====================")


