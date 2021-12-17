import pandas as pd
from TwitterAPI import TwitterAPI
import os
import tweepy as tw
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
import time
import datetime
from tqdm import tqdm

from flask import Flask,request
from flask_restful import Resource, Api, reqparse
import pandas as pd
import ast
import json

app = Flask(__name__)
api_flask = Api(app)


options = Options()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--no-proxy-server')
options.add_argument("--proxy-server='direct://'")
options.add_argument("--proxy-bypass-list=*")

sleep_on_rate_limit=False


consumer_key= 'jg0kH9QA20VKHlLOcosd7Vqcp'
consumer_secret= '3d9OfftAvdgvQYKJAYlkJwruQ3M3Hc77aexWP0IjCb5T0nYqFK'
access_token= '3293263684-9nUohBEDohlREGtWreeWBlyVS6STtjWLvMrgp2r'
access_token_secret= '1NmcBPb5Bj8aHK9Mcz8CR6gDjx0tefji8xYA7N8f7Pi4M'

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)




class tweeterApi(Resource):
    def get (self):
        args = request.args
        search_words =args["candidat"]
        tweets = tw.Cursor(api.search_tweets,
                q=search_words,
                lang="fr",
                tweet_mode="extended").items(500)
        list_of_tweets=[]
        for item in tweets:
            list_of_tweets.append(item._json)
        print(len(list_of_tweets))
        return list_of_tweets

api_flask.add_resource(tweeterApi, '/get_twitter')

if __name__ == '__main__':
    app.run()
