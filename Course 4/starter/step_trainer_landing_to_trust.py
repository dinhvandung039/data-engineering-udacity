import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step trainer landing
Steptrainerlanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://human-analytics/step_trainer_landing/"],
        "recurse": True,
    },
    transformation_ctx="Steptrainerlanding_node1",
)

# Script generated for node Customer curated
Customercurated_node1678180550487 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://human-analytics/customer_curated/"],
        "recurse": True,
    },
    transformation_ctx="Customercurated_node1678180550487",
)

# Script generated for node Inner join after distinct
SqlQuery0 = """
SELECT stl.* FROM step_trainer_landing stl 
INNER JOIN 
(
    SELECT DISTINCT serialnumber FROM customer_curated
) AS cc
ON stl.serialnumber = cc.serialnumber;
"""
Innerjoinafterdistinct_node1678180728097 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "step_trainer_landing": Steptrainerlanding_node1,
        "customer_curated": Customercurated_node1678180550487,
    },
    transformation_ctx="Innerjoinafterdistinct_node1678180728097",
)

# Script generated for node Step trainer trusted
Steptrainertrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Innerjoinafterdistinct_node1678180728097,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://human-analyticshuman-analytics/step_trainer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="Steptrainertrusted_node3",
)

job.commit()