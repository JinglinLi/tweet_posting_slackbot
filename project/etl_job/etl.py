import pymongo
import time
from sqlalchemy import create_engine
import pandas as pd
import logging
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# wait until tweets were all collected and writen in MongoDB before starting etl job
time.sleep(120)  # seconds 

#----- extract data from MongoDB ------
def extract():
    """ extract data from MongoDB """
    # connect mongodb container of the same composer from python
    client = pymongo.MongoClient("mongodb")
    # get database
    db = client.tweets_stream_db
    # get collection
    collection = db.tweet_stream_json
    return collection

#----- transform data : sentiment analysis 
def transform(collection):
    """ 
    transform mongodb cursor into dataframe
    perform sentiment analysis 
    return dataframe with 'tweet' and 'sentiment' column
    """
    logging.warning('----------The datatype of collection.find() is ------------')
    logging.warning(type(collection.find()))  # collection.find() is of type <class 'pymongo.cursor.Cursor'>
    logging.warning('-----------------------')
    # pointer into dataframe
    df = pd.DataFrame(list(collection.find()))
    # allocate dataframe to return
    tweet_df = pd.DataFrame()
    # assign column 'tweets'
    tweet_df['tweets'] = df['text']
    # sentiment analysis and assign column 'sentiment'
    s  = SentimentIntensityAnalyzer() # vader sentiment analysis
    tweet_df['sentiment'] = [s.polarity_scores(x)['compound'] for x in tweet_df['tweets']]
    logging.warning('----------The table to be writen in psql is dataframe: ------------')
    logging.warning(tweet_df)  # collection.find() is of type <class 'pymongo.cursor.Cursor'>
    logging.warning('-----------------------')
    return tweet_df


#------------- load to postgres
def load(tweet_df): 
    """ extract data to postgresdb """
    # connect postgresdb container of the same composer from python
    pg_engine = create_engine('postgresql://postgres:password@postgresdb:5432/tweeter', echo=True)
    # create table tweets
    pg_engine.execute('''
        CREATE TABLE IF NOT EXISTS tweets (
        text VARCHAR(500),
        sentiment NUMERIC
    );
    ''')
    # write dataframe into postgresdb table tweets
    tweet_df.to_sql('tweets', pg_engine, if_exists='replace')


# ETL
load(transform(extract()))