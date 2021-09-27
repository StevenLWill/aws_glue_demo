from datetime import datetime
from pyspark.context import SparkContext
import pyspark.sql.functions as f

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session

glue_db = "glue-demo-db"
glue_tbl = "read"
s3_write_path = "s3://glue-demo-bucket-indeed/write"

#Log starting time
dt_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("Start time: ",dt_start)

#Read movie data toe Glue dynamic frame
dynamic_frame_read = glue_context.create_dynamic_frame.from_catalog(database = glue_db, table_name = glue_tbl)

#Convert dynamic frame to data frame
data_frame = dynamic_frame_read.toDF()

##################################
#           TRANSFORM            #
##################################

#Create a decade column from year
decade_col = f.floor(data_frame["year"]/10)*10
data_frame = data_frame.withColumn("decade",decade_col)

#Group by decade: Count and Avg Rating
data_frame_aggregated = data_frame.groupby("decade").agg(f.count(f.col("movie_title")).alias("movie_count"),f.mean(f.col("rating")).alias("avg_rating"))

#Sort by number of movies per decade
data_frame_aggregated = data_frame_aggregated.orderBy(f.desc("movie_count"))

#Print result table_name
#Note: Show function is an action. Actions force the execution of the data frame plan.
#With big data the slowdown would be significant without cacching
data_frame_aggregated.show(10)


##################################
#             LOAD               #
##################################

#Only one partition is needed (small data)
data_frame_aggregated = data_frame_aggregated.repartition(1)

#Convert back to dynamic frame
dynamic_frame_write = DynamicFrame.fromDF(data_frame_aggregated, glue_context, "dynamic_frame_write")

#Write to S3
glue_context.write_dynamic_frame.from_options(frame=dynamic_frame_write,connection_type="s3", connection_options={"path":s3_write_path},format="csv")

#Log end time
dt_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("End time: ",dt_end)