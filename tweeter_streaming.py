# # Databricks notebook source
# !pip install tweepy
# # COMMAND ----------
# import tweepy
# # COMMAND ----------
# import matplotlib
# # COMMAND ----------
# import seaborn as sns
# COMMAND ----------
from tweepy import OAuthHandler, Stream
# COMMAND ----------
from tweepy.streaming import StreamListener
import socket
import json
# COMMAND ----------
consumer_key='PHENEzVPYMosaUrtts7v4OagQ'
consumer_token='B6o1VpVj5yTLeKw5iNMzIpy6KqgGsu1bHaZ42Xv4cVqvkMnb5y'
access_token ='1257835549227995138-hiZM0GzGYRSGDVIU9oNMuprcaLHiAK'
access_secret='AvOz0ESjomcCFEQF7mBVY4JebLxtsLvS6t8nNM91IlUUl'
# COMMAND ----------
class TweetListener(StreamListener):
  def __init__(self, csocket):
    self.client_socket= csocket
  
  def on_data(self,data):
    try:
      msg= json.loads(data)
      print(msg['text'].encode('utf-8'))
      self.client_socket.send(msg['text'].encode('utf-8'))
      return True
    except BaseException as e:
      print('ERROR ',e)
    return True
    
  def on_error(self, status):
    print(status)
    return True

# COMMAND ----------

def sendData(c_socket):
  auth= OAuthHandler(consumer_key, consumer_token)
  auth.set_access_token(access_token, access_secret)
  twitter_stream =Stream(auth, TweetListener(c_socket))
  twitter_stream.filter(track=['machine learning'])

# COMMAND ----------

if __name__ =='__main__':
  s= socket.socket()
  host= '127.0.0.1'
  port= 5555
  try:
    while True:
      s.bind((host, port))
      print('Listening on port 5555')
      s.listen(5)
      c,addr=s.accept()
      sendData(c)
  except KeyboardInterrupt:
    print('Teminating ')

    exit
# COMMAND ----------



