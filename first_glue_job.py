import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


location_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://yellow-taxi-pipeline-data/dimension_data/location/"], "recurse": True},
    format="csv",
    format_options={
        "withHeader": True,
    },
    transformation_ctx="location_df",
)
print(location_df.printSchema())

location_mapped = location_df.apply_mapping(
    [
        ("LocationID", "string", "location_id", "short"),
        ("Borough", "string", "borough", "string"),
        ("Zone", "string", "zone", "string"),
        ("service_zone", "string", "service_zone", "string"),
    ],
    transformation_ctx="location_mapped",
)
print(location_mapped.printSchema())

# Script generated for node Amazon Redshift
location_redshift_df = glueContext.write_dynamic_frame.from_options(
    frame=location_mapped,
    connection_type="redshift",
    connection_options = {
        "url": "jdbc:redshift://my-redshift-cluster.cezy0qn980ta.us-east-1.redshift.amazonaws.com:5439/dev",
        "dbtable": "public.location_dim",
        "user": "awsuser",
        "password": "Temp1234",
        "redshiftTmpDir": "s3://smartcity-queries/temp/",
        "aws_iam_role": "arn:aws:iam::471112663332:role/role-s3-to-redshift-vice-versa",
        "preactions": "CREATE TABLE IF NOT EXISTS public.location_dim (location_id SMALLINT, borough VARCHAR(20), zone VARCHAR(50), service_zone VARCHAR(15));",
    },
    transformation_ctx="location_redshift_df",
)


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


df = location_df.toDF()
df_pd = df.toPandas()

print("================ GLUE JOB START ====================")
print(df_pd)
print('test script has successfully ran')
print("================ GLUE JOB END ====================")