#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc


# In[ ]:


sc = SparkContext()


# In[ ]:


ssc = StreamingContext(sc, 10)


# In[ ]:


sqlcontext= SQLContext(sc)


# In[ ]:


socket_stream= ssc.socketTextStream('127.0.0.1', 5555)
if socket_stream == 0:
   print("Port is open")
else:
   print("Port is not open")
#sock.close()


# In[ ]:


lines= socket_stream.window(20)


# In[ ]:


from collections import namedtuple
flds = {'tag', 'count'}
tweet = namedtuple('Tweet', flds)


# In[ ]:


(lines.flatMap(lambda text:text.split(" "))
 .filter(lambda word: word.lower().startswith('@'))
 .map(lambda word: (word.lower(),1))
 .reduceByKey(lambda a,b: a+b)
 .map(lambda rec: Tweet(rec[0], rec[1]))
 .foreachRDD(lambda rdd: rdd.toDF().sort(desc("count"))
 .limit(10).registerTemplate("tweets"))
)


# In[ ]:


ssc.start()


# In[ ]:


import time
from IPython import display
import matplotlib.pyplot as plt
import seaborn as sns
#%matplotlib inline


# In[ ]:


count = 0
while count < 10:
    time.sleep(3)
    top_10_tweets = sqlcontext.sql('select tag, count from tweets')
    top_10_df= top_10_tweets.toPandas()
    display.clear_output(wait=True)
    sns.plt.figure(figsize = (10,8))
    sns.barplot(x='count', y='tag', data=top_10_df)
    sns.plt.show()
    count = count + 1


# In[ ]:


ssc.stop()


# In[ ]:




