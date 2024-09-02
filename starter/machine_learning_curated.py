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

# Script generated for node step trainer trusted
steptrainertrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="steptrainertrusted_node1",
)

# Script generated for node accelerometer trusted
accelerometertrusted_node1678261934484 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometertrusted_node1678261934484",
)

# Script generated for node SQL Query
SqlQuery0 = """
SELECT 
    act.timestamp,
    x, 
    y, 
    z,
    stt.sensorreadingtime,
    stt.serialnumber,
    stt.distancefromobject
FROM step_trainer_trusted stt
INNER JOIN accelerometer_trusted act
ON CAST(stt.sensorreadingtime AS VARCHAR(20)) = CAST(act.timestamp AS VARCHAR(20));
"""
SQLQuery_node1678261971818 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "step_trainer_trusted": steptrainertrusted_node1,
        "accelerometer_trusted": accelerometertrusted_node1678261934484,
    },
    transformation_ctx="SQLQuery_node1678261971818",
)

# Script generated for node machine learning curated
machinelearningcurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1678261971818,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://human-analytics/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="machinelearningcurated_node3",
)

job.commit()