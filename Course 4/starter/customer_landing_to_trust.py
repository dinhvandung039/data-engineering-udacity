import sys
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing Zone
CustomerLandingZone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://human-analytics/customer_landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLandingZone_node1",
)

# Script generated for node ApplyFilterMapping
ApplyFilterMapping_node2 = Filter.apply(
    frame=CustomerLandingZone_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="ApplyFilterMapping_node2",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1679540688547 = DynamicFrame.fromDF(
    ApplyFilterMapping_node2.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1679540688547",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node3 = glueContext.getSink(
    path="s3://human-analytics/customer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrustedZone_node3",
)
CustomerTrustedZone_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
CustomerTrustedZone_node3.setFormat("json")
CustomerTrustedZone_node3.writeFrame(DropDuplicates_node1679540688547)
job.commit()