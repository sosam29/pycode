# Databricks notebook source
from pyspark.sql import SparkSession
  

# COMMAND ----------

spark = SparkSession.builder.appName("ls_Example").getOrCreate()

# COMMAND ----------

data = spark.sql("Select * from original_csv")

# COMMAND ----------

data.printSchema()

# COMMAND ----------

from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

# COMMAND ----------

data.columns


# COMMAND ----------

#output = assembler.transform(data)

# COMMAND ----------

#from pyspark.ml.feature import StringIndexer

# COMMAND ----------

#idxer= StringIndexer(inputCol='carrier', outputCol='car_idx')

# COMMAND ----------

#from pyspark.sql.functions import when

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

#data.show(5)
#data = data.withColumn("delayed", F.when((data.arr_delay > 15),1).otherwise(0))

# COMMAND ----------

#data = data.drop('arr_delay')

# COMMAND ----------

#data.printSchema()

# COMMAND ----------

#data = data.drop('sched_dep_time','arr_time','sched_arr_time','')
data.show(5)

# COMMAND ----------

data.columns

# COMMAND ----------

#data= data.drop('dep_time', 'sched_dep_time', 'dep_delay', 'sched_arr_time', 'arr_delay','tailnum','hour', 'minute','time_hour')

# COMMAND ----------

#data.printSchema()

# COMMAND ----------

#data= data.drop('arr_time')

# COMMAND ----------

#data.show(5)

# COMMAND ----------

#d1 = StringIndexer(inputCol='carrier', outputCol='carr_idx').fit(data)

# COMMAND ----------

#df1= d1.transform(data)

# COMMAND ----------

#df1.show()

# COMMAND ----------

#indexer1= StringIndexer(inputCol='origin', outputCol='org_idx').fit(df1)

# COMMAND ----------

#df2= indexer1.transform(df1)

# COMMAND ----------

#indexer2= StringIndexer(inputCol='dest', outputCol='dest_idx').fit(df2)

# COMMAND ----------

#df3= indexer2.transform(df2)

# COMMAND ----------

#df3.show(5)

# COMMAND ----------

vectorassember = VectorAssembler(inputCols=['Avg Session Length', 'Time on App', 'Time on Website', 'Length of Membership'], outputCol='features')

# COMMAND ----------

#type(vectorassember)

# COMMAND ----------

print(vectorassember)

# COMMAND ----------

output = vectorassember.transform(data)

# COMMAND ----------

#df3.printSchema()

# COMMAND ----------

#from pyspark.sql.types import IntegerType
#df3= df3.withColumn("air_time", df3['air_time'].cast(IntegerType()))

# COMMAND ----------

#df3.printSchema()

# COMMAND ----------

#output = vectorassember.transform(df3)

# COMMAND ----------

type(output)

# COMMAND ----------

output.show(5)

# COMMAND ----------

output.select('features').show()

# COMMAND ----------

output.printSchema()

# COMMAND ----------

final_data= output.select('features','Yearly Amount Spent')

# COMMAND ----------

final_data.show()

# COMMAND ----------

train_data, test_data = final_data.randomSplit([0.7, 0.3])

# COMMAND ----------

train_data.describe().show()

# COMMAND ----------

test_data.describe().show()

# COMMAND ----------

sc.version
#from   import Linea

# COMMAND ----------

from pyspark.mllib import linalg
from pyspark.ml.regression import LinearRegression

# COMMAND ----------

lr= LinearRegression(labelCol='Yearly Amount Spent')

# COMMAND ----------

lr_model = lr.fit(train_data)

# COMMAND ----------

test_results= lr_model.evaluate(test_data)

# COMMAND ----------

test_results.residuals.show()

# COMMAND ----------

test_results.rootMeanSquaredError

# COMMAND ----------

test_results.r2

# COMMAND ----------

unlabled_data = test_data.select('features')

# COMMAND ----------

unlabled_data.show(5)

# COMMAND ----------

prediction =lr_model.transform(unlabled_data)

# COMMAND ----------

prediction.show()

# COMMAND ----------


