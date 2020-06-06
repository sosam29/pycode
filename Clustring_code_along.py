# Databricks notebook source
from pyspark.sql import SparkSession
spark= SparkSession.builder.appName('CodeAlongClustering').getOrCreate()

# COMMAND ----------

data= spark.read.csv('/FileStore/tables/seeds_dataset.csv', header=True, inferSchema=True)

# COMMAND ----------

data.describe().show()

# COMMAND ----------

from pyspark.ml.clustering import KMeans

# COMMAND ----------

from pyspark.ml.feature import  VectorAssembler

# COMMAND ----------

data.columns

# COMMAND ----------

ass = VectorAssembler(inputCols=data.columns, outputCol='features')

# COMMAND ----------

final_data= ass.transform(data)

# COMMAND ----------

final_data.show(1)

# COMMAND ----------

from pyspark.ml.feature import StandardScaler

# COMMAND ----------

scaler = StandardScaler(inputCol='features', outputCol='scaledfeatures')

# COMMAND ----------

scaler_model= scaler.fit(final_data).transform(final_data)

# COMMAND ----------

scaler_model.head(1)

# COMMAND ----------

kmeans= KMeans(featuresCol='scaledfeatures', k=3)

# COMMAND ----------

model= kmeans.fit(scaler_model)

# COMMAND ----------

print('WSSSE')

# COMMAND ----------

print(model.computeCost(scaler_model))

# COMMAND ----------

centers = model.clusterCenters()

# COMMAND ----------

print(centers)

# COMMAND ----------

model.transform(scaler_model).select('prediction').show()

# COMMAND ----------


