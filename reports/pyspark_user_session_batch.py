import json,os,sys,csv
from configparser import ConfigParser,ExtendedInterpolation
from pymongo import MongoClient
from bson.objectid import ObjectId
import logging
import logging.handlers
from logging.handlers import TimedRotatingFileHandler
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import array, col, explode, lit, struct 
from pyspark.sql import DataFrame
from typing import Iterable 
from functools import reduce
from operator import add
from pyspark import StorageLevel
import boto3
from kafka import KafkaConsumer, KafkaProducer
from bson import json_util
from datetime import date, timedelta, datetime
today = date.today()
currentDate =  today.strftime("%d.%m.%Y")

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

formatter = logging.Formatter('%(asctime)s - %(levelname)s')

successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)


# Add the log message handler to the logger
successHandler = logging.handlers.RotatingFileHandler(
    config.get('LOGS', 'user_session_success')
)
successBackuphandler = TimedRotatingFileHandler(
    config.get('LOGS','user_session_success'),
    when="w0",
    backupCount=1
)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = logging.handlers.RotatingFileHandler(
    config.get('LOGS', 'user_session_error')
)
errorBackuphandler = TimedRotatingFileHandler(
    config.get('LOGS', 'user_session_error'),
    when="w0",
    backupCount=1
)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)

try:
    def convert_to_row(d: dict) -> Row:
        return Row(**OrderedDict(sorted(d.items())))
except Exception as e:
    errorLogger.error(e, exc_info=True)

spark = SparkSession.builder.appName("user_reports").config(
    "spark.driver.memory", "50g"
).config(
    "spark.executor.memory", "100g"
).config(
    "spark.memory.offHeap.enabled", True
).config(
    "spark.memory.offHeap.size", "32g"
).getOrCreate()

sc = spark.sparkContext

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
hadoop_conf.set("fs.s3a.endpoint", config.get("S3","df_aws_region"))
hadoop_conf.set("fs.s3a.access.key", config.get("S3","aws_access_key"))
hadoop_conf.set("fs.s3a.secret.key", config.get("S3","aws_secret_key"))
hadoop_conf.set("fs.s3a.multiobjectdelete.enable","false")

s3_presigned_client = boto3.client('s3', 
                      aws_access_key_id=config.get("S3","aws_access_key"),
                      aws_secret_access_key=config.get("S3","aws_secret_key"),
                      region_name=config.get("S3","aws_region")
                      )
s3_client = boto3.resource("s3",
         aws_access_key_id=config.get("S3","aws_access_key"),
         aws_secret_access_key=config.get("S3","aws_secret_key"))
s3_bucket = s3_client.Bucket(config.get("S3","bucket_name"))

client = MongoClient(config.get('MONGO', 'mongo_url'))

user_db = client[config.get('MONGO', 'user_database_name')]
users_collec = user_db[config.get('MONGO', 'users_collection')]

mentoring_db = client[config.get('MONGO','mentoring_database_name')]
session_attendees_collec = mentoring_db[config.get('MONGO','session_attendees_collection')]
sessions_collec = mentoring_db[config.get('MONGO','sessions_collection')]


##Mentor data fetch from DB
mentor_sessions_cursorMongo = sessions_collec.aggregate(
        [{"$match": {"deleted": {"$exists":True,"$ne":None}}},
                 {
                  "$project": {
                          "_id": {"$toString": "$_id"},
                          "userId": {"$toString": "$userId"},
                          "status":1,
                          "feedbacks":{"_id":{"$toString": "$_id"},"questionId":{"$toString": "$questionId"},"value":1,"label":1},
                          "createdAt":1,
                          "startDateUtc":1,
                          "endDateUtc":1,
                          "title":1,
                          "recommendedFor":{"value":1,"label":1},
                          "categories":{"value":1,"label":1},
                          "medium":{"value":1,"label":1}
                  }
                 }
         ])

# Create mentor schema 
mentor_sessions_schema = StructType([
        StructField("_id", StringType(), True),
        StructField("userId", StringType(), True),
        StructField("status",StringType(),True),
        StructField("feedbacks",
          ArrayType(
            StructType([
                        StructField("_id", StringType(), True),
                        StructField("questionId", StringType(), True),
                        StructField("value", StringType(), True),
                        StructField("label", StringType(), True)
                       ])
        ), True),
        StructField("createdAt",TimestampType(),True),
        StructField("startDateUtc",StringType(),True),
        StructField("endDateUtc",StringType(),True),
        StructField("title",StringType(),True),
        StructField("recommendedFor",
          ArrayType(
            StructType([
                        StructField("value", StringType(), True),
                        StructField("label", StringType(), True)
                       ])
        ), True),
        StructField("categories",
          ArrayType(
            StructType([
                        StructField("value", StringType(), True),
                        StructField("label", StringType(), True)
                       ])
        ), True),
        StructField("medium",
          ArrayType(
            StructType([
                        StructField("value", StringType(), True),
                        StructField("label", StringType(), True)
                       ])
        ), True)
])
mentor_sessions_rdd = spark.sparkContext.parallelize(list(mentor_sessions_cursorMongo))
sessions_df = spark.createDataFrame(mentor_sessions_rdd,mentor_sessions_schema)

sessions_df = sessions_df.withColumn("startDateUtc_timestamp",to_timestamp(col("startDateUtc")))\
                         .withColumn("endDateUtc_timestamp",to_timestamp(col("endDateUtc")))\
                         .withColumn("Duration_of_session_min",round((col("endDateUtc_timestamp").cast("long") - col("startDateUtc_timestamp").cast("long"))/60))
mentor_sessions_df = sessions_df.select(F.col("_id").alias("sessionId"),"userId","status","feedbacks")

mentoru_sessions_df = mentor_sessions_df.groupBy("userId").agg(count(when(F.col("status") == "completed",True))\
                       .alias("No_of_sessions_conducted"),F.count("sessionId").alias("No_of_session_created"))

users_cursorMongo = users_collec.aggregate(
        [{"$match": {"deleted": {"$exists":True,"$ne":None}}},
         {
          "$project": {
             "_id": {"$toString": "$_id"},
             "name": 1,
             "isAMentor": 1,
             "location": {"_id":{"$toString": "$_id"},"value":1,"label":1},
             "designation":{"_id":{"$toString": "$_id"},"value":1,"label":1},
             "experience":1,
             "areasOfExpertise":{"_id":{"$toString": "$_id"},"value":1,"label":1}
           }
         }
        ])
        
users_schema = StructType([
    StructField("_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("isAMentor",BooleanType(),True),
    StructField("location",
        ArrayType(
            StructType([StructField("_id",StringType(),True),
                StructField("value",StringType(),True),
                StructField("label",StringType(),True)])
            ),True),
    StructField("designation",
        ArrayType(
            StructType([StructField("_id",StringType(),True),
                StructField("value",StringType(),True),
                StructField("label",StringType(),True)])
            ),True),
    StructField("experience",StringType(),True),
    StructField("areasOfExpertise",
        ArrayType(
            StructType([StructField("_id",StringType(),True),
                 StructField("value",StringType(),True),
                 StructField("label",StringType(),True)])
            ),True)
    ])

users_rdd = spark.sparkContext.parallelize(list(users_cursorMongo))
users_df = spark.createDataFrame(users_rdd,users_schema)

users_df = users_df.withColumn("exploded_location",F.explode_outer(F.col("location")))

users_df = users_df.select(F.col("_id").alias("UUID"),
                           F.col("name").alias("User Name"),
                           F.col("exploded_location.label").alias("State"),
                           concat_ws(",",F.col("designation.label")).alias("Designation"),
                           F.col("experience").alias("Years_of_Experience"),
                           concat_ws(",",F.col("areasOfExpertise.label")).alias("Areas_of_Expertise")
           )
mentee_users_df = users_df.select("UUID","User Name","State","Designation","Years_of_Experience")

mentor_user_sessions_df = mentoru_sessions_df.join(users_df,users_df["UUID"] == mentor_sessions_df["userId"],how="left")\
                .select(users_df["*"],mentoru_sessions_df["No_of_session_created"],mentoru_sessions_df["No_of_sessions_conducted"])
mentor_user_sessions_df = mentor_user_sessions_df.na.fill(0,subset=["No_of_session_created","No_of_sessions_conducted"])

session_attendees_cursorMongo = session_attendees_collec.aggregate(
        [{"$match": {"deleted": {"$exists":True,"$ne":None}}},
         {
          "$project": {
             "_id": {"$toString": "$_id"},
             "userId": {"$toString": "$userId"},
             "isSessionAttended":1,
             "feedbacks":{"_id":{"$toString": "$_id"},"questionId":{"$toString": "$questionId"},"value":1,"label":1},
             "sessionId":{"$toString": "$sessionId"}
          }
         }
        ])

session_attendees_schema = StructType([
    StructField("_id", StringType(), True),
    StructField("userId", StringType(), True),
    StructField("isSessionAttended",BooleanType(),True),
    StructField("feedbacks",
          ArrayType(
            StructType([
                        StructField("_id", StringType(), True),
                        StructField("questionId", StringType(), True),
                        StructField("value", StringType(), True),
                        StructField("label", StringType(), True)
                       ])
    ), True),
    StructField("sessionId",StringType(),True)
    ])

session_attendees_rdd = spark.sparkContext.parallelize(list(session_attendees_cursorMongo))

session_attendees_df = spark.createDataFrame(session_attendees_rdd,session_attendees_schema)


session_attendees_df_fd = session_attendees_df.filter(size("feedbacks")>=1)

if (session_attendees_df_fd.count() >=1) :
 session_attendees_df_fd = session_attendees_df_fd.withColumn("exploded_feedbacks",F.explode_outer(F.col("feedbacks")))
 
 session_attendees_df_fd_sr = session_attendees_df_fd.filter(F.col("exploded_feedbacks.label").isin(\
                             "How would you rate the Audio/Video quality?","How would you rate the engagement in the session?",\
                             "How would you rate the host of the session?"))
 
 session_attendees_df_fd_sr = session_attendees_df_fd_sr.groupBy("_id","sessionId").pivot("exploded_feedbacks.label")\
                             .agg(F.first("exploded_feedbacks.value"))
 
 
 session_attendees_df_fd_sr = session_attendees_df_fd_sr.groupBy("sessionId")\
                             .agg(round(avg(F.col("How would you rate the Audio/Video quality?")),2).alias("How would you rate the Audio/Video quality?"),\
                             round(avg(F.col("How would you rate the engagement in the session?")),2).alias("How would you rate the engagement in the session?"),\
                             round(avg(F.col("How would you rate the host of the session?")),2).alias("How would you rate the host of the session?"))
 
 session_attendees_df_fd = session_attendees_df_fd.filter(F.col("exploded_feedbacks.label").isin("How would you rate the host of the session?","How would you rate the engagement in the session?"))
 session_attendees_df_fd = session_attendees_df_fd.groupBy("_id","userId","isSessionAttended").pivot("exploded_feedbacks.label")\
                          .agg(F.first("exploded_feedbacks.value"))
 
 session_attendees_df_fd = session_attendees_df_fd.groupBy("userId").agg(
    round(avg(F.col("How would you rate the host of the session?")), 2).alias("How would you rate the host of the session?"),
    round(avg(F.col("How would you rate the engagement in the session?")), 2).alias("How would you rate the engagement in the session?")
 )
 
 user_avg_mentor_rating_columns = [F.col("How would you rate the host of the session?"), F.col("How would you rate the engagement in the session?")]
 
 session_attendees_df_fd = session_attendees_df_fd.na.fill(0).withColumn(
    "Avg_Mentor_rating",
    round(
        reduce(add, [x for x in user_avg_mentor_rating_columns])/len(user_avg_mentor_rating_columns),
        2
    )
)
 

 final_mentor_user_sessions_df = mentor_user_sessions_df.join(session_attendees_df_fd,\
                                mentor_user_sessions_df["UUID"]==session_attendees_df_fd["userId"],how="left")\
                                .select(mentor_user_sessions_df["*"],session_attendees_df_fd["How would you rate the host of the session?"],\
                                session_attendees_df_fd["How would you rate the engagement in the session?"],\
                                session_attendees_df_fd["Avg_Mentor_rating"]).persist(StorageLevel.MEMORY_AND_DISK)
 final_mentor_user_sessions_df = final_mentor_user_sessions_df.na.fill(0,subset=["How would you rate the host of the session?",\
                                "How would you rate the engagement in the session?"])
 
# update mentor headers
 final_mentor_user_sessions_df = final_mentor_user_sessions_df.withColumnRenamed("User Name","Mentor Name").withColumnRenamed("How would you rate the host of the session?","Mentor Rating").withColumnRenamed("How would you rate the engagement in the session?","Session Engagement Rating")

 final_mentor_user_sessions_df.repartition(1).write.format("csv").option("header",True).mode("overwrite").save(
    config.get("S3","mentor_user_path")+"temp/"
 )
else:
 #  update mentor headers
 mentor_user_sessions_df = mentor_user_sessions_df.withColumnRenamed("User Name","Mentor Name").withColumnRenamed("How would you rate the host of the session?","Mentor Rating").withColumnRenamed("How would you rate the engagement in the session?","Session Engagement Rating")
 
 mentor_user_sessions_df.repartition(1).write.format("csv").option("header",True).mode("overwrite").save(
    config.get("S3","mentor_user_path")+"temp/"
 )

## Mentee User Report
mentee_session_attendees_df = session_attendees_df.select(F.col("_id").alias("sessionAttendeesId"),"userId","isSessionAttended","feedbacks")
mentee_session_attendees_df = mentee_session_attendees_df.groupBy("userId").agg(count(when(F.col("isSessionAttended") == True,True))\
                       .alias("No_of_sessions_attended"),F.count("sessionAttendeesId").alias("No_of_sessions_enrolled"))
mentee_user_session_attendees_df = mentee_session_attendees_df.join(mentee_users_df,mentee_users_df["UUID"] == mentee_session_attendees_df["userId"],how="left")\
                .select(mentee_users_df["*"],mentee_session_attendees_df["No_of_sessions_enrolled"],mentee_session_attendees_df["No_of_sessions_attended"])
mentee_user_session_attendees_df = mentee_user_session_attendees_df.na.fill(0,subset=["No_of_sessions_enrolled",\
                                   "No_of_sessions_attended"])

# update mentee fields 
mentee_user_session_attendees_df = mentee_user_session_attendees_df.withColumnRenamed("User Name","Mentee Name").withColumnRenamed("No_of_sessions_enrolled","No. of sessions enrolled in")
mentee_user_session_attendees_df.repartition(1).write.format("csv").option("header",True).mode("overwrite").save(
    config.get("S3","mentee_user_path")+"temp/"
)

## Session Report
session_attendees_df_sr = session_attendees_df.groupBy("sessionId")\
                          .agg(F.count("_id").alias("No_of_Mentees_enrolled"),\
                          count(when(F.col("isSessionAttended") == True,True)).alias("No_of_Mentees_attended_the_session"),\
                          count(when(size("feedbacks")>=1,True)).alias("No_of_Mentees_who_gave_feedback"))

final_sessions_df = sessions_df.select(col("_id").alias("sessionId"),col("userId").alias("Host_UUID"),col("createdAt").alias("Time_stamp_of_session_creation"),\
                    col("startDateUtc").alias("Time_stamp_of_session_scheduled"),col("Duration_of_session_min"),\
                    col("title").alias("Title_of_the_session"),\
                    concat_ws(",",F.col("recommendedFor.label")).alias("Recommended_for"),\
                    concat_ws(",",F.col("categories.label")).alias("Categories"),\
                    concat_ws(",",F.col("medium.label")).alias("Languages"))


final_sessions_df = final_sessions_df.join(users_df, col("Host_UUID") == col("UUID"), "left") \
    .select(
        final_sessions_df["sessionId"],
        final_sessions_df["Host_UUID"],
        users_df["User Name"].alias("Mentor Name"),
        final_sessions_df["Time_stamp_of_session_creation"],
        final_sessions_df["Time_stamp_of_session_scheduled"],
        final_sessions_df["Duration_of_session_min"],
        final_sessions_df["Title_of_the_session"],
        final_sessions_df["Recommended_for"],
        final_sessions_df["Categories"],
        final_sessions_df["Languages"]
    )

final_sessions_df = final_sessions_df.join(session_attendees_df_sr,\
                    final_sessions_df["sessionId"] == session_attendees_df_sr["sessionId"],how="left")\
                    .select(final_sessions_df["*"],session_attendees_df_sr["No_of_Mentees_enrolled"],\
                    session_attendees_df_sr["No_of_Mentees_attended_the_session"],\
                    session_attendees_df_sr["No_of_Mentees_who_gave_feedback"])
final_sessions_df = final_sessions_df.na.fill(0,subset=["No_of_Mentees_enrolled","No_of_Mentees_attended_the_session",\
                    "No_of_Mentees_who_gave_feedback"])


if (session_attendees_df_fd.count() >=1) :
 final_sessions_df_sr = final_sessions_df.join(session_attendees_df_fd_sr,\
                       final_sessions_df["sessionId"] == session_attendees_df_fd_sr["sessionId"],how="left")\
                       .select(final_sessions_df["*"],session_attendees_df_fd_sr["How would you rate the Audio/Video quality?"],\
                       session_attendees_df_fd_sr["How would you rate the engagement in the session?"],\
                       session_attendees_df_fd_sr["How would you rate the host of the session?"])
 final_sessions_df_sr = final_sessions_df_sr.na.fill(0,subset=["How would you rate the Audio/Video quality?",\
                       "How would you rate the engagement in the session?","How would you rate the host of the session?"])
 
 # update sessions headers
 final_sessions_df_sr = final_sessions_df_sr.withColumnRenamed("How would you rate the Audio/Video quality?","Audio/Video quality rating").withColumnRenamed("How would you rate the engagement in the session?","Session Engagement Rating").withColumnRenamed("Host_UUID","Mentor UUID").withColumnRenamed("How would you rate the host of the session?","Mentor Rating")

 final_sessions_df_sr.repartition(1).write.format("csv").option("header",True).mode("overwrite").save(
    config.get("S3","session_path")+"temp/"
 )
else:
 # update sessions headers
 final_sessions_df = final_sessions_df.withColumnRenamed("How would you rate the Audio/Video quality?","Audio/Video quality rating").withColumnRenamed("How would you rate the engagement in the session?","Session Engagement Rating").withColumnRenamed("Host_UUID","Mentor UUID").withColumnRenamed("How would you rate the host of the session?","Mentor Rating")
 
 final_sessions_df.repartition(1).write.format("csv").option("header",True).mode("overwrite").save(
    config.get("S3","session_path")+"temp/"
 )
##Generating Pre-signed url for all the reports stored in S3
expiryInSec = config.get("S3","expiryTime")
s3_session_folder = config.get("S3","s3_session_folder")
s3_mentor_user_folder = config.get("S3","s3_mentor_user_folder")
s3_mentee_user_folder = config.get("S3","s3_mentee_user_folder")

session_fileName = None
mentorUser_fileName = None
menteeUser_fileName = None
for f in s3_bucket.objects.filter(Prefix=s3_mentor_user_folder):
    if str(f.key.split('/')[-1]).endswith(".csv"):
     mentorUser_fileName = f.key.split('/')[-1]
for f in s3_bucket.objects.filter(Prefix=s3_mentee_user_folder):
    if str(f.key.split('/')[-1]).endswith(".csv"):
     menteeUser_fileName = f.key.split('/')[-1]
for f in s3_bucket.objects.filter(Prefix=s3_session_folder):
    if str(f.key.split('/')[-1]).endswith(".csv"):
     session_fileName = f.key.split('/')[-1]

reportPrefix = ""
if "dev_reports" in s3_session_folder or "dev_reports" in s3_mentor_user_folder or "dev_reports" in s3_mentee_user_folder:
  reportPrefix = "DEV-"
else:
  reportPrefix = ""

# Copying file to rename 
source_object = {
    'Bucket': config.get("S3","bucket_name"),
    'Key': s3_mentor_user_folder+"temp/"+mentorUser_fileName
}

destination_object = s3_mentor_user_folder+str(reportPrefix)+"Mentor User Report_"+str(currentDate)+".csv"
copy_source = source_object


s3_client.meta.client.copy(copy_source,config.get("S3","bucket_name"), destination_object)

mentor_user_presigned_url = s3_presigned_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": config.get("S3","bucket_name"), "Key": destination_object},
        ExpiresIn = int(expiryInSec)
    )

# Copying file to rename 
source_object = {
    'Bucket': config.get("S3","bucket_name"),
    'Key': s3_mentee_user_folder+"temp/"+menteeUser_fileName
}

destination_object = s3_mentee_user_folder+str(reportPrefix)+"Mentee User Report_"+str(currentDate)+".csv"
copy_source = source_object

s3_client.meta.client.copy(copy_source,config.get("S3","bucket_name"), destination_object)


mentee_user_presigned_url = s3_presigned_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": config.get("S3","bucket_name"), "Key": destination_object},
        ExpiresIn = int(expiryInSec)
    )

# Copying file to rename 
source_object = {
    'Bucket': config.get("S3","bucket_name"),
    'Key': s3_session_folder+"temp/"+session_fileName
}

destination_object = s3_session_folder+str(reportPrefix)+"Session Report_"+str(currentDate)+".csv"
copy_source = source_object

s3_client.meta.client.copy(copy_source,config.get("S3","bucket_name"), destination_object)

session_presigned_url = s3_presigned_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": config.get("S3","bucket_name"), "Key": destination_object},
        ExpiresIn = int(expiryInSec)
    )


# open the file in the write mode
with open('output.csv', 'w') as f:
    # create the csv writer
    writer = csv.writer(f)
    # write a row to the csv file
    writer.writerow(["mentor_user_presigned_url","mentee_user_presigned_url","session_presigned_url"])
    writer.writerow([mentor_user_presigned_url,mentee_user_presigned_url,session_presigned_url])

# Send Email
kafka_producer = KafkaProducer(bootstrap_servers=config.get("KAFKA","kafka_url"))

email_data = {"type":"email","email":{"to":config.get("EMAIL","to"),"cc":config.get("EMAIL","cc"),"subject": config.get("EMAIL","subject"),"body":"<div style='margin:auto;width:50%'><p style='text-align:center'><img style='height:250px;' class='cursor-pointer' alt='MentorED' src='"+config.get('EMAIL','logo')+"'></p><div><p>Hello , </p> Please find the User and Session Reports Attachment Links below ...<p><table style='width:50%'><tr><td><b>Mentor User Report</b></td><td><a href='"+mentor_user_presigned_url+"'> click here</a></td></tr><tr><td><b>Mentee User Report</b></td><td><a href='"+mentee_user_presigned_url+"'> click here</a></td></tr><tr><td><b>Session Report</b></td><td><a href='"+session_presigned_url+"'> click here</a></td></tr></table></div><div style='margin-top:100px'><div>Thanks & Regards</div><div>Team MentorED</div><div style='margin-top:20px;color:#b13e33'><div><p>Note:- </p><ul><li>FYI, The Attachment Link shared above will be having the expiry duration. Please use it before expires</li><li>Do not reply to this email. This email is sent from an unattended mailbox. Replies will not be read.</div><div>For any queries, please feel free to reach out to us at "+config.get("EMAIL","supportEmail")+"</li></ul></div></div></div></div>"}}
kafka_producer.send(config.get("KAFKA","notification_kafka_topic_name"), json.dumps(email_data, default=json_util.default).encode('utf-8'))
kafka_producer.flush()