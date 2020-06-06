# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName('Churn_or_not').getOrCreate()

# COMMAND ----------

data = spark.read.csv('/FileStore/tables/customer_churn.csv', header=True, inferSchema=True)

# COMMAND ----------

data.columns

# COMMAND ----------

from pyspark.ml.classification  import LogisticRegression

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

# COMMAND ----------

vectorizer= VectorAssembler(inputCols=['Age', 'Total_Purchase', 'Account_Manager', 'Years', 'Num_Sites'], outputCol='features')

# COMMAND ----------

my_data= vectorizer.transform(data)

# COMMAND ----------

my_data.printSchema()


# COMMAND ----------

selected_data= my_data.select('features', 'churn')

# COMMAND ----------

selected_data.show(5)

# COMMAND ----------

from pyspark.ml.regression import LinearRegression

# COMMAND ----------

lr= LinearRegression(labelCol='churn', featuresCol='features')

# COMMAND ----------

train_lr, test_lr= selected_data.randomSplit([0.7, 0.3])

# COMMAND ----------

train_lr.describe().show()

# COMMAND ----------

test_lr.describe().show()

# COMMAND ----------

lr_model = lr.fit(train_lr)

# COMMAND ----------

lr_eval_result= lr_model.evaluate(test_lr)

# COMMAND ----------

training_sum =lr_model.summary

# COMMAND ----------

training_sum.predictions.describe().show()

# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator

# COMMAND ----------

pred_and_label= lr_model.evaluate(test_lr)

# COMMAND ----------

pred_and_label.predictions.show()

# COMMAND ----------

churn_eval = BinaryClassificationEvaluator(rawPredictionCol='prediction', labelCol='churn')

# COMMAND ----------

auc= churn_eval.evaluate(pred_and_label.predictions)

# COMMAND ----------

auc

# COMMAND ----------

#lr_eval_result.r2

# COMMAND ----------

#
#lr_eval_result.predictions

# COMMAND ----------

#lr_eval_result.predictions.show()

# COMMAND ----------

#training_sum= lr_model.summary

# COMMAND ----------

#training_sum.predictions.describe().show()

# COMMAND ----------

#from pyspark.ml.evaluation import BinaryClassificationEvaluator

# COMMAND ----------

#pred_and_label= lr_model.evaluate(test_lr)

# COMMAND ----------

#pred_and_label.predictions.show()

# COMMAND ----------


