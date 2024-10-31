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

# Script generated for node Customer Trusted
CustomerTrusted_node1728493094153 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-analytics/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1728493094153")

# Script generated for node Accelerometer landing
Accelerometerlanding_node1728493148635 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-analytics/accelerometer/landing/"], "recurse": True}, transformation_ctx="Accelerometerlanding_node1728493148635")

# Script generated for node Join
Join_node1728456339820 = Join.apply(frame1=CustomerTrusted_node1728493094153, frame2=Accelerometerlanding_node1728493148635, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1728456339820")

# Script generated for node Drop Fields
SqlQuery0 = '''
select x, y, z, user, timestamp from join_table
'''
DropFields_node1728456922702 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"join_table":Join_node1728456339820}, transformation_ctx = "DropFields_node1728456922702")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1728456703154 = glueContext.getSink(path="s3://stedi-human-balance-analytics/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1728456703154")
AccelerometerTrusted_node1728456703154.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1728456703154.setFormat("json")
AccelerometerTrusted_node1728456703154.writeFrame(DropFields_node1728456922702)
job.commit()
