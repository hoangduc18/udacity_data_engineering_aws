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
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Step-Trainer Landing
StepTrainerLanding_node1728493410208 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-analytics/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1728493410208")

# Script generated for node Customer curated
Customercurated_node1728493456195 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-analytics/customer/curated/"], "recurse": True}, transformation_ctx="Customercurated_node1728493456195")

# Script generated for node Join and Remove Duplicates
SqlQuery0 = '''
select distinct s.sensorreadingtime, s.serialnumber, s.distancefromobject
from step_trainer s join customer_curated c
on s.serialnumber = c.serialnumber;
'''
JoinandRemoveDuplicates_node1728465117005 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer":StepTrainerLanding_node1728493410208, "customer_curated":Customercurated_node1728493456195}, transformation_ctx = "JoinandRemoveDuplicates_node1728465117005")

# Script generated for node Step-Trainer Trusted
StepTrainerTrusted_node1728465358133 = glueContext.getSink(path="s3://stedi-human-balance-analytics/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1728465358133")
StepTrainerTrusted_node1728465358133.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1728465358133.setFormat("json")
StepTrainerTrusted_node1728465358133.writeFrame(JoinandRemoveDuplicates_node1728465117005)
job.commit()
