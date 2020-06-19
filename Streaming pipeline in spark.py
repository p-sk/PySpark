#!/usr/bin/env python
# coding: utf-8

# # Exercise : 
# ## Createing topic in Kafka for customer
# 
KAFKA_SERVER = "localhost:6667"
TOPIC = 'customer'

#Import Modules
from pyspark.sql import SparkSession 
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.sql.types import FloatType,IntegerType
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier

#SPark session 
spark = SparkSession.builder.master("local").appName("ML LIb").config("spark.some.config.option", "some-value").getOrCreate()

# Loading data 
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_SERVER) \
  .option("subscribe", TOPIC) \
  .load()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")


#Data Pre-Processing 
categoricalCol = 'type'
stages = []
stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + 'out')
encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "Vec"])
stages += [stringIndexer, encoder]
numericCols = ['amount','oldbalanceOrg','newbalanceOrig','oldbalanceDest','newbalanceDest','isFlaggedFraud']
for i in numericCols:
  df = df.withColumn(i, df[i].cast(FloatType()))
df = df.withColumn("label", df['isFraud'].cast(IntegerType()))
assemblerInputs = [categoricalCol + "Vec"] + numericCols
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [assembler]


#pipeline creation 
pipeline = Pipeline(stages = stages)

pipelineModel = pipeline.fit(df)
df = pipelineModel.transform(df)


#Loading Model which were saved in exc 2 
gbtModel = GBTClassifier.load("gbtModel.model")




#Predicting Value 
predictions = gbtModel.transform(test)


# File location and type
file_location = "dataset.csv"
file_type = "csv"

# CSV options
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type).option("header", first_row_is_header)   .option("sep", delimiter)   .load(file_location)


#Create a spark Wtite Stream to save data to kafka 


ds = df.selectExpr("CAST(step AS STRING)",
                   "CAST(type AS STRING)","CAST(amount AS STRING)","CAST(nameOrig AS STRING)",
                   "CAST(oldbalanceOrg AS STRING)","CAST(newbalanceOrig AS STRING)",
                   "CAST(nameDest AS STRING)","CAST(oldbalanceDest AS STRING)",
                   "CAST(newbalanceDest AS STRING)",
                   "CAST(isFraud AS STRING)","CAST(isFlaggedFraud AS STRING)").write\
        .format("kafka")\
        .option("kafka.bootstrap.servers", KAFKA_SERVER)\
        .option("topic", TOPIC).save() 


