import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
import pyspark
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from awsglue.transforms import *
from awsglue.job import Job
from pyspark.sql.functions import col, avg

#### ###creating spark and gluecontext ###############
def transform_and_save(database_name, table_name):
    sc = pyspark.SparkContext()
    gluecontext = GlueContext(sc)
    spark = gluecontext.spark_session
    job = Job(gluecontext)

    print("Job Execution Started…")

    ## ###Creating glue dynamic frame from the catalog ###########
    input_dyf = gluecontext.create_dynamic_frame.from_catalog(database = database_name, table_name = table_name)

    ## convert the DynamicFrame to a DataFrame
    input_df = input_dyf.toDF()

    ## Transforming the data
    transformed_df = input_df.filter(input_df.suppressed.isNull()) \
                            .filter(input_df.period.like("2011%")) \
                            .selectExpr("series_reference", "period", "CAST(data_value.long AS DOUBLE) AS data_value", "units") \
                            .groupBy("period") \
                            .agg(avg("data_value").alias("avg_data_value"))

    ## Converting the transformed DataFrame back to a DynamicFrame
    transformed_dyf = DynamicFrame.fromDF(transformed_df, gluecontext, "transformed_dyf")

    ## Writing the transformed data to the S3 bucket
    s3_output_path = "s3://glue-serverless-demo/output-data/"
    gluecontext.write_dynamic_frame.from_options(frame = transformed_dyf, connection_type = "s3", connection_options = {"path": s3_output_path}, format = "csv")

    job.commit()

transform_and_save("glue-serverless-demo-db", "input_data")
