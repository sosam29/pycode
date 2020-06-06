# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName("CruiseShip").getOrCreate()

# COMMAND ----------

data = spark.sql("Select * from cruise_ship_info_csv")

# COMMAND ----------

data.show(5)

# COMMAND ----------

from pyspark.ml.feature import StringIndexer

# COMMAND ----------

indexer = StringIndexer(inputCol='Cruise_line', outputCol='cruise_idx').fit(data)

# COMMAND ----------

data= indexer.transform(data)

# COMMAND ----------

data.show(5)

# COMMAND ----------

from pyspark.ml import linalg
from pyspark.ml.feature import VectorAssembler

# COMMAND ----------

vector= VectorAssembler(inputCols=['Age','Tonnage','passengers','length','cabins','passenger_density','cruise_idx'], outputCol='features')

# COMMAND ----------

data= vector.transform(data)

# COMMAND ----------

data.show(5)

# COMMAND ----------

data= data.select('features','crew')

# COMMAND ----------

data.show(5)

# COMMAND ----------

train_data, test_data= data.randomSplit([0.7, 0.3])

# COMMAND ----------

train_data.describe().show()

# COMMAND ----------

test_data.describe().show()

# COMMAND ----------

from pyspark.ml.regression import LinearRegression

# COMMAND ----------

lr= LinearRegression(labelCol='crew')

# COMMAND ----------

train_ship_model = lr.fit(train_data)

# COMMAND ----------

ship_Result= train_ship_model.evaluate(test_data)

# COMMAND ----------

ship_Result.rootMeanSquaredError

# COMMAND ----------

train_data.describe().show()

# COMMAND ----------

print(ship_Result)

# COMMAND ----------

for s in ship_Result:
  print(s)

# COMMAND ----------

df =ship_Result.residuals.show()


# COMMAND ----------

ship_Result.degreesOfFreedom

# COMMAND ----------

from pyspark.sql.functions import corr

# COMMAND ----------

data.select(corr('passengers','crew')).show()

# COMMAND ----------


