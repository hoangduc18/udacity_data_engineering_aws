import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1728370758170 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-analytics/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1728370758170")

# Script generated for node Privacy Filter
PrivacyFilter_node1728371027862 = Filter.apply(frame=AmazonS3_node1728370758170, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="PrivacyFilter_node1728371027862")

# Script generated for node Trusted Customer Zone
TrustedCustomerZone_node1728371123443 = glueContext.getSink(path="s3://stedi-human-balance-analytics/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="TrustedCustomerZone_node1728371123443")
TrustedCustomerZone_node1728371123443.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
TrustedCustomerZone_node1728371123443.setFormat("json")
TrustedCustomerZone_node1728371123443.writeFrame(PrivacyFilter_node1728371027862)
job.commit()