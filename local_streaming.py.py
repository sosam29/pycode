
# In[4]:


import sys
from pyspark.sql  import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


# In[6]:


#host = 'localhost'
#port = 5555


# In[7]:


spark = SparkSession.builder.appName('wordcounter').getOrCreate()


# In[8]:


spark.sparkContext.setLogLevel('ERROR')


# In[16]:


lines = spark.readStream.format('socket').option('host', 'localhost').option('port', 5555).load()


# In[17]:


words = lines.select(explode (split(lines.value,' ')).alias('word'))


# In[18]:


wordCounts= words.groupBy('word').count()


# In[19]:


query = wordCounts.writeStream.outputMode('complete').format('console').start()


# In[ ]:


query.awaitTermination()


# In[ ]:




