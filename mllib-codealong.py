# Databricks notebook source
from pyspark.sql import SparkSession
  

# COMMAND ----------

spark = SparkSession.builder.appName("ls_Example").getOrCreate()

# COMMAND ----------

data = spark.sql("Select * from flights_csv")

# COMMAND ----------

data.printSchema()

# COMMAND ----------

from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

# COMMAND ----------

data.columns


# COMMAND ----------

#assembler = VectorAssembler(inputCols=['arr_delay', 'carrier', 'flight', 'origin', 'dest', 'air_time', 'distance'], outputCol='features')

# COMMAND ----------

#output = assembler.transform(data)

# COMMAND ----------

from pyspark.ml.feature import StringIndexer

# COMMAND ----------

idxer= StringIndexer(inputCol='carrier', outputCol='car_idx')

# COMMAND ----------

from pyspark.sql.functions import when

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

#data.show(5)
data = data.withColumn("delayed", F.when((data.arr_delay > 15),1).otherwise(0))

# COMMAND ----------

#data = data.drop('arr_delay')

# COMMAND ----------

data.printSchema()

# COMMAND ----------

#data = data.drop('sched_dep_time','arr_time','sched_arr_time','')
data.show(5)

# COMMAND ----------

data.columns

# COMMAND ----------

data= data.drop('dep_time', 'sched_dep_time', 'dep_delay', 'sched_arr_time', 'arr_delay','tailnum','hour', 'minute','time_hour')

# COMMAND ----------

data.printSchema()

# COMMAND ----------

data= data.drop('arr_time')

# COMMAND ----------

data.show(5)

# COMMAND ----------

d1 = StringIndexer(inputCol='carrier', outputCol='carr_idx').fit(data)

# COMMAND ----------

df1= d1.transform(data)

# COMMAND ----------

df1.show()

# COMMAND ----------

indexer1= StringIndexer(inputCol='origin', outputCol='org_idx').fit(df1)

# COMMAND ----------

df2= indexer1.transform(df1)

# COMMAND ----------

indexer2= StringIndexer(inputCol='dest', outputCol='dest_idx').fit(df2)

# COMMAND ----------

df3= indexer2.transform(df2)

# COMMAND ----------

df3.show(5)

# COMMAND ----------

vectorassember = VectorAssembler(inputCols=['air_time','distance','carr_idx','org_idx','dest_idx'], outputCol='features')

# COMMAND ----------

type(vectorassember)

# COMMAND ----------

print(vectorassember)

# COMMAND ----------

output = vectorassember.transform(df3)

# COMMAND ----------

df3.printSchema()

# COMMAND ----------

from pyspark.sql.types import IntegerType
df3= df3.withColumn("air_time", df3['air_time'].cast(IntegerType()))

# COMMAND ----------

df3.printSchema()

# COMMAND ----------

output = vectorassember.transform(df3)

# COMMAND ----------

type(output)

# COMMAND ----------

output.show(5)

# COMMAND ----------

output.select('features').show()

# COMMAND ----------

output.printSchema()

# COMMAND ----------

final_data= output.select('features','delayed')

# COMMAND ----------

final_data.show()

# COMMAND ----------

train_data, test_data = final_data.randomSplit([0.7, 0.3])

# COMMAND ----------

train_data.describe().show()

# COMMAND ----------

train_data.show()

# COMMAND ----------


