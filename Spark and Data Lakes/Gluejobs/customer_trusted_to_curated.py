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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1728458951885 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1728458951885")

# Script generated for node Customer Trusted
CustomerTrusted_node1728459010656 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1728459010656")

# Script generated for node Join
Join_node1728459045529 = Join.apply(frame1=CustomerTrusted_node1728459010656, frame2=AccelerometerLanding_node1728458951885, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1728459045529")

# Script generated for node Drop Fields and Drop Duplicates
SqlQuery6572 = '''
select distinct customerName, email, phone,
birthDay, serialNumber, registrationDate, 
lastUpdateDate, shareWithPublicAsOfDate,
shareWithResearchAsOfDate, shareWithFriendsAsOfDate
from temp_table

'''
DropFieldsandDropDuplicates_node1728459091243 = sparkSqlQuery(glueContext, query = SqlQuery6572, mapping = {"temp_table":Join_node1728459045529}, transformation_ctx = "DropFieldsandDropDuplicates_node1728459091243")

# Script generated for node Customer Curated
CustomerCurated_node1728459398275 = glueContext.getSink(path="s3://stedi-human-balance-analytics/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1728459398275")
CustomerCurated_node1728459398275.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1728459398275.setFormat("json")
CustomerCurated_node1728459398275.writeFrame(DropFieldsandDropDuplicates_node1728459091243)
job.commit()