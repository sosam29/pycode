# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName("Logistic_regress").getOrCreate()

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression

# COMMAND ----------

data = spark.read.format('libsvm').load('/FileStore/tables/sample_libsvm_data.txt')

# COMMAND ----------

data.show()

# COMMAND ----------

mulog_reg_model = LogisticRegression()

# COMMAND ----------

log_reg_model = mulog_reg_model.fit(data)

# COMMAND ----------

log_summary= log_reg_model.summary

# COMMAND ----------

log_summary.predictions.printSchema()

# COMMAND ----------

train_data, test_data = data.randomSplit([0.7, 0.3])

# COMMAND ----------

final_model = LogisticRegression()
fit_final = final_model.fit(train_data)

# COMMAND ----------

prediction_and_label= fit_final.evaluate(test_data)

# COMMAND ----------

prediction_and_label.predictions.show()

# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

# COMMAND ----------

binary_eval = BinaryClassificationEvaluator()

# COMMAND ----------

bin_eval_result =binary_eval.evaluate(prediction_and_label.predictions)

# COMMAND ----------

bin_eval_result

# COMMAND ----------

multi_evaluator = MulticlassClassificationEvaluator()

# COMMAND ----------

#type(prediction_and_label)
multi_evaluator_result = multi_evaluator.evaluate(,labelCol=prediction_and_label.predictions['label'] )

# COMMAND ----------

prediction_and_label.predictions['label']

# COMMAND ----------


