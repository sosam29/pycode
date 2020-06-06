# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName('spamdetect').getOrCreate()

# COMMAND ----------

data = spark.read.csv('/FileStore/tables/SMSSpamCollection', inferSchema=True, sep='\t')

# COMMAND ----------

data.show(5)

# COMMAND ----------

data=data.withColumnRenamed('_c0','class').withColumnRenamed('_c1','text')

# COMMAND ----------

data.show(5)

# COMMAND ----------

from pyspark.sql.functions import length

# COMMAND ----------

data= data.withColumn('length', length(data['text']))

# COMMAND ----------

data.show(5)

# COMMAND ----------

data.groupBy('class').mean().show()

# COMMAND ----------

from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF, StringIndexer

# COMMAND ----------

tokenizer = Tokenizer(inputCol='text', outputCol='token_text')
stop_remover=StopWordsRemover(inputCol='token_text', outputCol='stop_token')
count_vec= CountVectorizer(inputCol='stop_token', outputCol='c_vec')
idf = IDF(inputCol='c_vec', outputCol='tf_idf')
ham_or_spam= StringIndexer(inputCol='class', outputCol='label')


# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

# COMMAND ----------

vector= VectorAssembler(inputCols=['tf_idf','length'], outputCol='features')

# COMMAND ----------

from pyspark.ml.classification import DecisionTreeClassifier

# COMMAND ----------

dct=DecisionTreeClassifier(maxBins=2)

# COMMAND ----------

dct.extractParamMap()

# COMMAND ----------

from pyspark.ml import Pipeline

# COMMAND ----------

data_pre_pipeline = Pipeline(stages=[ham_or_spam, tokenizer, stop_remover, count_vec, idf,vector] )

# COMMAND ----------

cleaner =data_pre_pipeline.fit(data)

# COMMAND ----------

clean_data= cleaner.transform(data)

# COMMAND ----------

clean_data= clean_data.select('label','features')

# COMMAND ----------

train,test= clean_data.randomSplit([0.7, 0.3])

# COMMAND ----------

spam_detctor= dct.fit(train)

# COMMAND ----------

test_result=spam_detctor.transform(test)

# COMMAND ----------

test_result.show()

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# COMMAND ----------

acc_eval = MulticlassClassificationEvaluator()

# COMMAND ----------

acc = acc_eval.evaluate(test_result)

# COMMAND ----------

print(acc)

# COMMAND ----------


