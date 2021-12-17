import praw
import csv
import re
import json
import requests
import mysql.connector
import traceback
import sys
import threading
import DatabaseHandler

from datetime import datetime
from dateutil import tz

from tqdm import tqdm


class StockPost(object):
    def __init__(self, postID, postURL, ups, downs, numComments, stock, date):
        self.postID = postID
        self.url = postURL
        self.stock = stock
        self.ups = ups
        self.downs = downs
        self.numComments = numComments
        self.date = date

    def json_enc(self):
        return {'stock': self.stock, 'postID': self.postID, 'postURL': self.url, 'ups': self.ups, 'downs': self.downs,
                'numComments': self.numComments}


def json_def_encoder(obj):
    if hasattr(obj, 'json_enc'):
        return obj.json_enc()
    else:  # some default behavior
        return obj.__dict__


class SubredditScraper:

    def __init__(self, sub, sort='new', lim=900):
        self.sub = sub
        self.sort = sort
        self.lim = lim
        self.reddit = praw.Reddit()

        # print(
        # f'SubredditScraper instance created with values '
        # f'sub = {sub}, sort = {sort}, lim = {lim}')

    def set_sort(self):
        if self.sort == 'new':
            return self.sort, self.reddit.subreddit(self.sub).new(limit=self.lim)
        elif self.sort == 'top':
            return self.sort, self.reddit.subreddit(self.sub).top(limit=self.lim)
        elif self.sort == 'hot':
            return self.sort, self.reddit.subreddit(self.sub).hot(limit=self.lim)
        else:
            self.sort = 'hot'
            print('Sort method was not recognized, defaulting to hot.')
            return self.sort, self.reddit.subreddit(self.sub).hot(limit=self.lim)

    def get_posts(self, stocklist):

        stock_tickers = {}

        if stocklist == ["stocks"]:
            with open('./../Tickers/tickers_stocks.csv', mode='r') as infile:
                reader = csv.reader(infile)
                for row in reader:
                    stock_tickers[row[0].split(',')[0]] = {}

        elif stocklist == ["crypto"]:
            with open('./../Tickers/tickers_crypto.csv', mode='r') as infile:
                reader = csv.reader(infile)
                for row in reader:
                    stock_tickers[row[0].split(',')[0]] = {}

        else:
            for ticker in stocklist:
                stock_tickers[ticker] = {}

        """Get unique posts from a specified subreddit."""

        # Attempt to specify a sorting method.
        sort, subreddit = self.set_sort()

        # print(f'Collecting information from r/{self.sub}. Sort by ({self.sort})')

        # Search posts for tickers #
        relevant_posts = []
        subreddit = list(subreddit)

        for i in tqdm(range(len(subreddit)), desc="[1/2] Scraping Posts", leave=False):
            post = subreddit[i]
            if post.link_flair_text != 'Meme':
                for stock in stock_tickers.keys():
                    try:
                        if (re.search(r"\s+\$?" + stock + r"\$?\s+", post.selftext) or re.search(
                                r"\s+\$?" + stock + r"\$?\s+", post.title)):
                            stock_tickers[stock][post.id] = StockPost(post.id, post.permalink, post.ups, post.downs,
                                                                      post.num_comments, stock, post.created_utc)
                    except:
                        print(f"This Ticker threw an exception: {stock}")
                        traceback.print_exc()

        for stock in stock_tickers.keys():
            if len(stock_tickers[stock]) > 0:
                for post in stock_tickers[stock]:
                    relevant_posts.append(stock_tickers[stock][post])
                    # semap.acquire()
        # json_object = json.dumps(relevant_posts, default=json_def_encoder, indent = 4)
        # print(json_object)

        # Upload data to db #
        cnx = db_handler.connect_to_db("TheSpatula")
        mycursor = cnx.cursor()
        assert mycursor
        assert db_handler.table_exists(mycursor, "reddit")

        for x in tqdm(range(len(relevant_posts)), desc="[2/2] Updating Database", leave=False):
            post = relevant_posts[x]
            num_votes = post.ups + post.downs

            # get created_date and convert from utc to local time
            utc_stamp = post.date
            utc = datetime.utcfromtimestamp(utc_stamp).strftime('%Y-%m-%d %H:%M:%S')
            from_zone = tz.tzutc()
            to_zone = tz.tzlocal()
            utc = datetime.strptime(utc, '%Y-%m-%d %H:%M:%S')
            utc = utc.replace(tzinfo=from_zone)
            date_posted = utc.astimezone(to_zone).date()

            # Add post, if it exists already, update post #
            mycursor.execute(f"""
            INSERT INTO reddit (post_id, symbol, num_comments, num_votes, date_posted) 
            VALUES("{post.postID}", "{post.stock}", {post.numComments}, {num_votes}, "{date_posted}")
            ON DUPLICATE KEY UPDATE num_comments={post.numComments}, num_votes={num_votes}, date_posted="{date_posted}"
            ;""")

            cnx.commit()


# get_posts() every subreddit with 10000 post limit #
def deep_scrape(stocklist, sublist):
    if sublist == ["all"]:
        subreddits = ["CryptoCurrency", "CryptoMoonShots", "CryptoMarkets", "Crypto_com", "wallstreetbets",
                      "Wallstreetbetsnew", "stocks", "RobinHoodPennyStocks", "pennystocks", "weedstocks", "trakstocks",
                      "ausstocks", "shroomstocks", "Canadapennystocks"]
    elif sublist == ["stocks"]:
        subreddits = ["wallstreetbets", "Wallstreetbetsnew", "stocks", "RobinHoodPennyStocks", "pennystocks",
                      "weedstocks", "trakstocks", "ausstocks", "shroomstocks", "Canadapennystocks"]
    elif sublist == ["crypto"]:
        subreddits = ["CryptoCurrency", "CryptoMoonShots", "CryptoMarkets", "Crypto_com", "wallstreetbets",
                      "Wallstreetbetsnew"]
    else:
        subreddits = sublist

        threads = []
    for x in tqdm(range(len(subreddits)), desc="DEEP SCRAPE"):
        sub = subreddits[x]

        threads.append(
            threading.Thread(target=SubredditScraper(sub, lim=10000, sort='new').get_posts, args=[stocklist]))
        threads.append(
            threading.Thread(target=SubredditScraper(sub, lim=10000, sort='hot').get_posts, args=[stocklist]))
        threads.append(
            threading.Thread(target=SubredditScraper(sub, lim=10000, sort='top').get_posts, args=[stocklist]))

        for thread in threads[-3:]:
            print(f"Thread {threads.index(thread) + 1} Starting")
            thread.start()

        if sub == subreddits[-1]:
            for thread in threads:
                thread.join()
                print(f"Thread {threads.index(thread) + 1} Joined")


if __name__ == '__main__':
    # config by machine
    global db_handler
    db_handler = DatabaseHandler("win")

    # stocklist options:
    #    ["stocks"]         //all stocks on NYSE and Nasdaq
    #    ["crypto"]         //all crypto
    #    ["abc", "def"]     //custom
    stocklist = ["crypto"]

    # sublist options:
    #    ["all"]         //crypto and stock subs
    #    ["stocks"]      //stock subs
    #    ["crypto"]      //crypto subs
    #    ["abc", "def"]  //custom
    sublist = ["wallstreetbets"]

    deep_scrape(stocklist, sublist)
    # SubredditScraper('wallstreetbets', lim=20, sort='hot').get_posts(stocklist)

    print("DONE!!")
