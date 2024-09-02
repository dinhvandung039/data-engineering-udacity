import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://human-analytics/customer_landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1",
)

# Script generated for node Share With Research
ShareWithResearch_node2 = Filter.apply(
    frame=CustomerLanding_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="ShareWithResearch_node2",
)

# Script generated for node Drop Duplicates
DropDuplicates_node3 = ShareWithResearch_node2.toDF().dropDuplicates(["email"])

# Script generated for node Customer Trusted
DropDuplicates_node3_dynamic_frame = DynamicFrame.fromDF(
    DropDuplicates_node3,
    glueContext,
    "DropDuplicates_node3_dynamic_frame"
)

CustomerTrusted_node4 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node3_dynamic_frame,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://human-analytics/customer_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrusted_node4",
)

job.commit()