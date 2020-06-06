# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

from pyspark.ml.recommendation import ALS

# COMMAND ----------

from pyspark.ml.evaluation import  RegressionEvaluator

# COMMAND ----------

data = spark.read.csv('/FileStore/tables/movielens_ratings.csv', header=True, inferSchema=True)

# COMMAND ----------

spark = SparkSession.builder.appName('recommendersys').getOrCreate()

# COMMAND ----------

data.show()

# COMMAND ----------

data.describe().show()

# COMMAND ----------

train, test = data.randomSplit([0.7, 0.3])

# COMMAND ----------

als= ALS(maxIter=5, regParam=0.01, userCol='userId', itemCol='movieId', ratingCol='rating')

# COMMAND ----------

model= als.fit(train)

# COMMAND ----------

pred= model.transform(test)

# COMMAND ----------

pred.show()

# COMMAND ----------



# COMMAND ----------

eval = RegressionEvaluator(metricName='rmse', labelCol='rating', predictionCol='prediction')

# COMMAND ----------

rmse = eval.evaluate(pred)

# COMMAND ----------

print(rmse)

# COMMAND ----------


