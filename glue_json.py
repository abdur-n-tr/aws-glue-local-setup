import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.gluetypes import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Define schema matching the JSON object structure
schema = StructType([
    StructField("firstName", StringType()),
    StructField("lastName", StringType()),
    StructField("jobs", ArrayType(StructType([
        StructField("title", StringType()),
        StructField("company", StringType())
    ])))
])


json_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://ar-test-json-data/raw_data/"], "recurse": True},
    format="json",
    format_options={"withSchema": json.dumps(schema.jsonValue())},
    transformation_ctx="json_df",
)

data_frames = json_df.relationalize("root", "s3://ar-test-json-data/meta/")

print(data_frames)
print(data_frames.keys())

print(data_frames['root'].printSchema())
print(data_frames['root'].show())

print(data_frames['root_jobs'].printSchema())
print(data_frames['root_jobs'].show())
