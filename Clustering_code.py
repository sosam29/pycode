# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName("clustering").getOrCreate()

# COMMAND ----------

data = spark.read.format('libsvm').load('/FileStore/tables/sample_kmeans_data.txt')

# COMMAND ----------

from pyspark.ml.clustering import KMeans

# COMMAND ----------

final_data= data.select('features')

# COMMAND ----------

kmean = KMeans().setK(2).setSeed(1)

# COMMAND ----------

model = kmean.fit(final_data)

# COMMAND ----------

wssse= model.computeCost(final_data)

# COMMAND ----------

centera= model.clusterCenters()

# COMMAND ----------

centera

# COMMAND ----------


