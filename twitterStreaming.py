import sys
import socket
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import json


class TweetListener(StreamListener):

    def __init__(self, socket):
        print("Tweet Listener initalized")
        self.client_socket = socket
        
    def on_data(self, data):
        try:
            jsonMessage= json.loads(data)
            message= jsonMessage["text"].encode("utf-8")
            print(message)
            self.client_socket.send(message)
        except BaseException as e:
            print("Error on_data: %s" %str(e))
        
        return True
    
    def on_error(self,status):
        print(status)
        return True
    
def connect_to_twitter(connection, tracks):
    api_key="PHENEzVPYMosaUrtts7v4OagQ"
    api_secret="B6o1VpVj5yTLeKw5iNMzIpy6KqgGsu1bHaZ42Xv4cVqvkMnb5y"
    
    access_token="1257835549227995138-hiZM0GzGYRSGDVIU9oNMuprcaLHiAK"
    access_token_secret="AvOz0ESjomcCFEQF7mBVY4JebLxtsLvS6t8nNM91IlUUl"
    
    auth=OAuthHandler(api_key,api_secret)
    auth.set_access_token(access_token, access_token_secret)
    
    twitter_stream= Stream(auth,TweetListener(connection))
    twitter_stream.filter(track=tracks, languages=["en"])
    
    
if __name__=="__main__":
    if len(sys.argv) < 4:
        print("Usage: python twitterStreaming.py <host> <port> <traks>", file=sys.stderr)
        exit(-1)
        
    host= sys.argv[1]
    port= int(sys.argv[2])
    tracks= sys.argv[3:]
    
    s= socket.socket()
    s.bind((host, port))
    
    print("Listening on port : %s" % str(port))
    
    s.listen(5)
    
    connection, client_address= s.accept()
    print("Received reqest from: "+str(client_address))
    print("Initiating listener for these tracks: ", tracks)
    
    connect_to_twitter(connection, tracks)
    
    
        
        
    
    


