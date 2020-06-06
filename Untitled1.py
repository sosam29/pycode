#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark


# In[2]:


sc= pyspark.SparkContext()


# In[3]:


sql = SQLContext(sc)


# In[5]:


from pyspark.sql.context import SQLContext


# In[6]:


from pyspark.context import SparkContext


# In[7]:


from pyspark.sql import SparkSession


# In[8]:


spark = SparkSession .builder .appName('How to build csv File').getOrCreate()


# In[9]:


spark.version


# In[11]:


df = spark.read.csv("c:/Users/sosam/Downloads/flights.csv", header='true')


# In[12]:


df.head()


# In[15]:


df.printSchema


# In[16]:


type(df)


# In[17]:


df.show(5)


# In[20]:


df.columns[df.isNull.any()].sum()


# In[21]:


df.count()


# In[26]:


from pyspark.sql.functions import isnan, when, count,col


# In[27]:


df.select([count(when(isnan(c), c)).alias(c) for c in df.columns]).show()


# In[28]:


df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()


# In[ ]:





# In[23]:


df.select(df.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)): _*).show()


# In[29]:


df.drop('year')


# In[30]:


df.show()


# In[31]:


df.drop(df.year)


# In[32]:


df.drop(df.day)


# In[33]:


df.printSchema


# In[34]:


df.show(5)


# In[35]:


df=df.drop(df.year)


# In[36]:


df.show()


# In[37]:


df= df.drop('month','day','dep_time','sched_dep_time','dep_delay','arr_time','sched_arr_time')


# In[38]:


df.show(5)


# In[42]:


carrier_count =df.select(df[carrier]).distinct().count()


# In[43]:


df['carrier'].distinct().count()


# In[51]:


df = df.withColumn('delayed', (df.arr_delay >15))


# In[52]:


df.show(5)


# In[53]:


from pyspark.ml.feature import StringIndexer


# In[54]:


import pyspark.sql.functions as F


# In[57]:


df= df.withColumn('delayed', F.when((df.arr_delay>15),1).otherwise(0))


# In[60]:


df= df.drop('time_hour')


# In[62]:


df= df.drop('arr_delay')


# In[64]:


df= df.drop('arr_time')


# In[66]:


df= df.drop('flight')


# In[68]:


df.describe()


# In[69]:


df.describe


# In[77]:


from pyspark.ml.feature import OneHotEncoderEstimator


# In[78]:


encoder = OneHotEncoderEstimator(inputCols=['carrier', 'origin'], outputCols=['carrier_idx','origin_idx'])


# In[ ]:





# In[67]:


df.show()


# In[80]:


df.show()


# In[81]:


indexer=StringIndexer(inputCol='carrier', outputCol='carrier_idx').fit(df)


# In[83]:


type(indexer)


# In[84]:


indexer_df= indexer.transform(df)


# In[85]:


type(indexer_df)


# In[86]:


indexer_df.show(5)


# In[87]:


orgidxer_df= StringIndexer(inputCol='origin', outputCol='origin_idx').fit(indexer_df).transform(indexer_df)


# In[90]:


desidxer_df= StringIndexer(inputCol='dest', outputCol='dest_idx').fit(orgidxer_df).transform(orgidxer_df)


# In[91]:


desidxer_df.show(5)


# In[94]:


df2= desidxer_df.drop('carrier', 'origin','dest')


# In[95]:


df2.show(5)


# In[93]:


desidxer_df.show(5)


# In[101]:


desidxer_df.select(['air_time','distance','carrier_idx','origin_idx','dest_idx']).describe().show()


# In[103]:


desidxer_df.select(desidxer_df.air_time IsNull()')


# In[108]:


desidxer_df.select(desidxer_df.air_time =='NA')


# In[110]:


desidxer_df.describe().show()


# In[115]:


desidxer_df.select("tailnum===NA" || "tailnum === ''")


# In[118]:


desidxer_df.count()


# In[120]:


df3= desidxer_df.drop()


# In[123]:


df3.count()


# In[ ]:


schema = StrucType([StructField("carrier", StringType(), False]),                    StructField("tailnum", StringType(), False]),                    StructField("origin", StringType(), False]),                    StructField("dest", StringType(), False]),                    StructField("distance", IntegerType(), False]),                    StructField("carrier", StringType(), False]),                    StructField("carrier", StringType(), False]),

