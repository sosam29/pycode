# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName("TreesDemo").getOrCreate()

# COMMAND ----------

data = spark.read.format('libsvm').load('/FileStore/tables/sample_libsvm_data.txt')

# COMMAND ----------

data.describe().show()

# COMMAND ----------

data.show()

# COMMAND ----------

from pyspark.ml.classification import GBTClassifier, RandomForestClassifier, DecisionTreeClassifier

# COMMAND ----------

train_data, test_data= data.randomSplit([0.7, 0.3])

# COMMAND ----------

gbc_class = GBTClassifier()
rfc_class = RandomForestClassifier(numTrees=100)
dtc_class = DecisionTreeClassifier()

# COMMAND ----------

gbc_model= gbc_class.fit(train_data)
rfc_model= rfc_class.fit(train_data)
dtc_model= dtc_class.fit(train_data)

# COMMAND ----------

gbc_pred= gbc_model.transform(test_data)
rfc_pred= rfc_model.transform(test_data)
dtc_pred= dtc_model.transform(test_data)

# COMMAND ----------

#gbc_pred.show()

# COMMAND ----------

from pyspark.ml.evaluation import  MulticlassClassificationEvaluator

# COMMAND ----------

acc_Eval = MulticlassClassificationEvaluator(metricName="average_precision")

# COMMAND ----------

acc_Eval.evaluate(rfc_pred)

# COMMAND ----------


