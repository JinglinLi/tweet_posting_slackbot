import requests
from sqlalchemy import create_engine
import logging
import time
import pandas as pd

# wait until finishing tweet_collector and etl_job to start 
time.sleep(180)  # seconds 

# url get from slack developer webpage
webhook_url = "..."

# # This part is only used when want to post python joke on slack instead of tweets
# # If want to use this part requirements.txt should include pyjoke
# import pyjokes
# joke = pyjokes.get_joke()
# data = {'text': joke}
# requests.post(url=webhook_url, json = data)

# connect postgresdb container of the same docker composer from python
pg_engine = create_engine('postgresql://postgres:password@postgresdb:5432/tweeter', echo=True)

# get the content of tweets table in postgresdb
query = 'SELECT * FROM tweets;'
result = pg_engine.execute(query)
data_all = result.fetchall()
print('------This is the data from postgresql ------')
print(type(data_all)) # list of tuples [(ind, text, score), (ind, text, score), ...]
print(data_all)
print('---------------------')
# turn list of tuples into dataframe
df = pd.DataFrame(data_all, columns=['ind', 'tweet', 'sentiment'])
# find the most positive tweet and corresponding sentiment
text = df['tweet'].iloc[df['sentiment'].argmax()]
score = df['sentiment'].iloc[df['sentiment'].argmax()]
# edit the content of slack post
post = f'\n \n Hey check this tweet: \n {text} \n According to vader it is already the most positive one of last ten tweets with a sentiment score of: \n {score} \n \n'
logging.warning('------------- This is the post going to be posted on slack --------------')
logging.warning(post)
logging.warning(type(post))
#logging.warning(type(joke))
logging.warning('--------------------')
# post on slack
requests.post(url=webhook_url, json = {'text': post}) 