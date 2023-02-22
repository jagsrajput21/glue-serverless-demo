import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
import pyspark


#### ###creating spark and gluecontext ###############
def transform_and_save(database_name, table_name):
    sc = pyspark.SparkContext()
    gluecontext = GlueContext(sc)
    spark = gluecontext.spark_session
    job = Job(gluecontext)

    print("Job Execution Started…")

    ## ###Creating glue dynamic frame from the catalog ###########
    input_dyf = gluecontext.create_dynamic_frame.from_catalog(database = database_name, table_name = table_name)

    s3_output_path = "s3://glue-serverless-demo/output-data/"
    gluecontext.write_dynamic_frame.from_options(\
            frame = input_dyf, \
            connection_type = "s3", \
            connection_options = {"path": s3_output_path \
                }, format = "parquet")  

    job.commit()

transform_and_save("glue-serverless-demo-db", "input_data")
