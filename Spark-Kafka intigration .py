#!/usr/bin/env python
# coding: utf-8

# # Exercise 1: 
# ## Createing topic in Kafka for customer
# 

# In[3]:


#Importing lib from Kafka-python
from kafka.admin import KafkaAdminClient, NewTopic

KAFKA_SERVER = "localhost:6667"
CLIENT_ID = 'client1'

# Creating topic
'''
admin_client = KafkaAdminClient(
    bootstrap_servers=KAFKA_SERVER, 
    client_id=CLIENT_ID
    security_protocol='PLAINTEXT',
    api_version=1
)

topic_list = []
topic_list.append(NewTopic(name="customer", num_partitions=1, replication_factor=1))
admin_client.create_topics(new_topics=topic_list, validate_only=False)
'''


#Load Spark
from pyspark.sql import SparkSession 
spark = SparkSession.builder     .master("local")     .appName("ML LIb")     .config("spark.some.config.option", "some-value")     .getOrCreate()

# Loading data 

# File location and type
file_location = "dataset.csv"
file_type = "csv"

# CSV options
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type)   .option("header", first_row_is_header)   .option("sep", delimiter)   .load(file_location)


#Create a spark Stream to save data to kafka 

ds = df.select(to_json( struct( df.columns.map(col(_)):_*  ) )  as "value")\
.write.format("kafka")\
.option("kafka.bootstrap.servers", KAFKA_SERVER)\
.option("topic", "customer").save()  

'''
ds = df.selectExpr("CAST(step AS STRING)","CAST(type AS STRING)","CAST(amount AS STRING)","CAST(nameOrig AS STRING)","CAST(oldbalanceOrg AS STRING)","CAST(newbalanceOrig AS STRING)","CAST(nameDest AS STRING)","CAST(oldbalanceDest AS STRING)","CAST(newbalanceDest AS STRING)","CAST(isFraud AS STRING)","CAST(isFlaggedFraud AS STRING)").write   .format("kafka")   .option("kafka.bootstrap.servers", KAFKA_SERVER)   .option("topic", "customer").save()         
'''

