# Databricks notebook source
from pyspark.sql import SparkSession
spark =SparkSession.builder.appName('detect_hack').getOrCreate()

# COMMAND ----------

data = spark.read.csv('/FileStore/tables/hack_data.csv', header=True, inferSchema=True)

# COMMAND ----------

data.show()

# COMMAND ----------

from pyspark.ml.feature import  VectorAssembler

# COMMAND ----------

from pyspark.ml.clustering import KMeans

# COMMAND ----------

data.columns


# COMMAND ----------

assemler= VectorAssembler(inputCols=['Session_Connection_Time',
 'Bytes Transferred',
 'Kali_Trace_Used',
 'Servers_Corrupted',
 'Pages_Corrupted',
 'WPM_Typing_Speed'], outputCol='features')

# COMMAND ----------

model= assemler.transform(data)

# COMMAND ----------

model.show(5)

# COMMAND ----------

from pyspark.ml.feature import StandardScaler

# COMMAND ----------

scaler = StandardScaler(inputCol='features', outputCol='scaledfeature')

# COMMAND ----------

scaler_model= scaler.fit(model)

# COMMAND ----------

cluster_final_data= scaler_model.transform(model)

# COMMAND ----------

cluster_final_data.show(5)

# COMMAND ----------

kmeans2 = KMeans(featuresCol='scaledfeature', k=2)
kmeans3 = KMeans(featuresCol='scaledfeature', k=3)

# COMMAND ----------

model_kmeans2 = kmeans2.fit(cluster_final_data)
model_kmeans3 = kmeans3.fit(cluster_final_data)

# COMMAND ----------

model_kmeans3.transform(cluster_final_data).groupBy('prediction').count().show()

# COMMAND ----------

model_kmeans2.transform(cluster_final_data).groupBy('prediction').count().show()

# COMMAND ----------

m2= model_kmeans2.transform(cluster_final_data)

# COMMAND ----------

m3= model_kmeans3.transform(cluster_final_data)
count= m3.select('prediction').distinct().count()
print(count)

# COMMAND ----------


