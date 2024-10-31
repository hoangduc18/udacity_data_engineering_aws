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

# Script generated for node Step-Trainer Trusted
StepTrainerTrusted_node1728466192212 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1728466192212")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1728466261235 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1728466261235")

# Script generated for node Join Table And Drop Duplicates
SqlQuery6692 = '''
select s.sensorreadingtime, s.serialnumber,
s.distancefromobject, a.x, a.y, a.z, a.user
from step_trainer s join accelerometer a
on s.sensorreadingtime = a.timestamp

'''
JoinTableAndDropDuplicates_node1728466408511 = sparkSqlQuery(glueContext, query = SqlQuery6692, mapping = {"step_trainer":StepTrainerTrusted_node1728466192212, "accelerometer":AccelerometerTrusted_node1728466261235}, transformation_ctx = "JoinTableAndDropDuplicates_node1728466408511")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1728467558720 = glueContext.getSink(path="s3://stedi-human-balance-analytics/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1728467558720")
MachineLearningCurated_node1728467558720.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1728467558720.setFormat("json")
MachineLearningCurated_node1728467558720.writeFrame(JoinTableAndDropDuplicates_node1728466408511)
job.commit()