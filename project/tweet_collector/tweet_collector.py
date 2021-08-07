# This script is used to get tweet of a certain from twitter api and 
# write them into mongodb container in the docker composer. 
# This script import get_tweets.py as module


import get_tweets
import pymongo
from tweepy import Cursor, API
import logging
import datetime


# connect to mongodb container in the same docker composer
client = pymongo.MongoClient("mongodb")
# create database
db = client.tweets_db
# create collection
collection = db.tweet_json

# the following code is copied and modified based on get_tweets.py
auth = get_tweets.authenticate()
api = API(auth)

cursor = Cursor(
    api.user_timeline,
    id = 'AllenDowney',#'AndrewYNg',#'jjx33608175',#'elonmusk'
    tweet_mode = 'extended'
)

for status in cursor.items(2):
    text = status.full_text

    # take extended tweets into account
    # TODO: CHECK
    if 'extended_tweet' in dir(status):
        text =  status.extended_tweet.full_text
    if 'retweeted_status' in dir(status):
        r = status.retweeted_status
        if 'extended_tweet' in dir(r):
            text =  r.extended_tweet.full_text

    tweet = {
        'text': text,
        'username': status.user.screen_name,
        'followers_count': status.user.followers_count
    }
    
    # write tweet(dictionary) into mongodb 
    logging.warning('------------- Tweet being written into MongoDB --------------')
    logging.warning(tweet)
    collection.insert_one(tweet)
    logging.warning(str(datetime.datetime.now()))
    logging.warning('-----------------\n')
