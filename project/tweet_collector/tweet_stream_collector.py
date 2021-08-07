# This script is used to get stream of tweets from  twitter api and 
# write them into mongodb container in the docker compose. 
# This script is modified from get_tweets_streaming.py


import pymongo
import config
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
import logging
import datetime


# connect to mongodb container in the same docker composer
client = pymongo.MongoClient("mongodb")
# create database
db = client.tweets_stream_db
# create 'table'
collection = db.tweet_stream_json


def authenticate():
    """Function for handling Twitter Authentication. Please note
       that this script assumes you have a file called config.py
       which stores the 4 required authentication tokens:

       1. API_KEY
       2. API_SECRET
       3. ACCESS_TOKEN
       4. ACCESS_TOKEN_SECRET

    See course material for instructions on getting your own Twitter credentials.
    """
    auth = OAuthHandler(config.API_KEY, config.API_SECRET)
    auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET)

    return auth

class MaxTweetsListener(StreamListener):

    def __init__(self, max_tweets, *args, **kwargs):
        # initialize the StreamListener
        super().__init__(*args, **kwargs)
        # set the instance attributes
        self.max_tweets = max_tweets
        self.counter = 0
        
    def on_connect(self):
        print('connected. listening for incoming tweets')


    def on_status(self, status):
        """Whatever we put in this method defines what is done with
        every single tweet as it is intercepted in real-time"""
        
        # increase the counter
        self.counter += 1        

        tweet = {
            'text': status.text,
            'username': status.user.screen_name,
            'followers_count': status.user.followers_count
        }

        # write into mongodb
        logging.warning('------------- Tweet being written into MongoDB --------------')
        logging.warning(tweet)
        collection.insert_one(tweet)
        logging.warning(str(datetime.datetime.now()))
        logging.warning('-----------------\n')

        # check if we have enough tweets collected
        if self.max_tweets == self.counter:
            # reset the counter
            self.counter=0
            # return False to stop the listener
            return False


    def on_error(self, status):
        if status == 420:
            print(f'Rate limit applies. Stop the stream.')
            return False


if __name__ == '__main__':
    auth = authenticate()
    listener = MaxTweetsListener(max_tweets=10)
    stream = Stream(auth, listener)
    stream.filter(track=['berlin'], languages=['en'], is_async=False)