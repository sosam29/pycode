# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('dog_spoier').getOrCreate()

# COMMAND ----------

data= spark.read.csv("/FileStore/tables/dog_food.csv", header=True, inferSchema=True)

# COMMAND ----------

data.show(5)

# COMMAND ----------

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler

# COMMAND ----------

assembler = VectorAssembler(inputCols=['A','B','C','D'], outputCol='features')

# COMMAND ----------

fit_data= assembler.transform(data)

# COMMAND ----------

fit_data.show(5)

# COMMAND ----------

rfc = RandomForestClassifier(featuresCol='features', labelCol='Spoiled', numTrees=100)

# COMMAND ----------

final_data= fit_data.select('features', 'Spoiled')

# COMMAND ----------

final_data.show(5)

# COMMAND ----------

rfc = RandomForestClassifier(labelCol='Spoiled')

# COMMAND ----------

rfc_model = rfc.fit(final_data)

# COMMAND ----------

final_data.head(3)

# COMMAND ----------

rfc_model.featureImportances
