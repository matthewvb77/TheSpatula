import praw
import csv
import re

import constant
from concurrent.futures import ThreadPoolExecutor
import time
import DatabaseHandler

from datetime import datetime
from dateutil import tz


class StockPost(object):
    def __init__(self, postID, postURL, ups, downs, numComments, stock, date):
        assert isinstance(postID, str) and postID, "postID must be a non-empty string"
        assert isinstance(postURL, str) and postURL, "postURL must be a non-empty string"
        assert isinstance(stock, str) and stock, "stock must be a non-empty string"
        assert isinstance(ups, int) and ups >= 0, "ups must be a non-negative integer"
        assert isinstance(downs, int) and downs >= 0, "downs must be a non-negative integer"
        assert isinstance(numComments, int) and numComments >= 0, "numComments must be a non-negative integer"
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
        assert isinstance(sub, str) and sub, "sub must be a non-empty string"
        assert sort in ['new', 'top', 'hot'], "sort must be one of 'new', 'top', or 'hot'"
        assert isinstance(lim, int) and lim > 0, "lim must be a positive integer"
        self.sub = sub
        self.sort = sort
        self.lim = lim
        self.reddit = praw.Reddit()

        # print(
        # f'SubredditScraper instance created with values '
        # f'sub = {sub}, sort = {sort}, lim = {lim}')

    def set_sort(self):
        assert isinstance(self.sort, str), "Sort must be a string"
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

    def get_posts(self, stock_list):
        
        assert isinstance(stock_list, list) and all(isinstance(stock, str) for stock in stock_list), "stock_list must be a list of strings"
        stock_tickers = {}

        if stock_list == ["stocks"]:
            with open('./../Tickers/tickers_stocks.csv', mode='r') as infile:
                reader = csv.reader(infile)
                for row in reader:
                    stock_tickers[row[0].split(',')[0]] = {}

        elif stock_list == ["crypto"]:
            cnx = DatabaseHandler.connect_to_db(user, "TheSpatula")
            mycursor = cnx.cursor()
            assert mycursor
            assert DatabaseHandler.table_exists(mycursor, "crypto_symbols")
            mycursor.execute(f"""SELECT symbol FROM crypto_symbols;""")
            for row in mycursor:
                # mycursor SELECT always returns a tuple
                stock_tickers[row[0]] = {}

        else:
            for ticker in stock_list:
                stock_tickers[ticker] = {}

        """Get unique posts from a specified subreddit."""

        # Attempt to specify a sorting method.
        sort, subreddit = self.set_sort()

        # print(f'Collecting information from r/{self.sub}. Sort by ({self.sort})')

        # Search posts for tickers #
        relevant_posts = []
        subreddit = list(subreddit)

        for i in range(len(subreddit)):
            post = subreddit[i]
            if post.link_flair_text != 'Meme':
                for stock in stock_tickers.keys():
                    if (re.search(r"\s+\$?" + re.escape(stock) + r"\$?\s+", post.selftext) or re.search(
                            r"\s+\$?" + re.escape(stock) + r"\$?\s+", post.title)):
                        stock_tickers[stock][post.id] = StockPost(post.id, post.permalink, post.ups, post.downs,
                                                                  post.num_comments, stock, post.created_utc)

        for stock in stock_tickers.keys():
            if len(stock_tickers[stock]) > 0:
                for post in stock_tickers[stock]:
                    relevant_posts.append(stock_tickers[stock][post])

        # json_object = json.dumps(relevant_posts, default=json_def_encoder, indent = 4)
        # print(json_object)

        # Upload data to db #
        """ NOTE: MySQL connector is NOT thread-safe. Every thread needs it's own instance."""
        cnx = DatabaseHandler.connect_to_db(user, "TheSpatula")
        mycursor = cnx.cursor()
        assert mycursor
        assert DatabaseHandler.table_exists(mycursor, "reddit")

        for x in range(len(relevant_posts)):
            post = relevant_posts[x]
            num_votes = post.ups + post.downs

            # get created_date and convert from utc to local time
            utc_stamp = post.date
            utc = datetime.utcfromtimestamp(utc_stamp).strftime('%Y-%m-%d %H:%M:%S')
            from_zone = tz.tzutc()
            to_zone = tz.tzlocal()
            utc = datetime.strptime(utc, '%Y-%m-%d %H:%M:%S')
            utc = utc.replace(tzinfo=from_zone)
            date_posted = utc.astimezone(to_zone)

            # Add post, if it exists already, update post #
            mycursor.execute(f"""
            INSERT INTO reddit (post_id, symbol, num_comments, num_votes, date_posted) 
            VALUES("{post.postID}", "{post.stock}", {post.numComments}, {num_votes}, "{date_posted}")
            ON DUPLICATE KEY UPDATE num_comments={post.numComments}, num_votes={num_votes}, date_posted="{date_posted}"
            ;""")

            cnx.commit()


# get_posts() every subreddit with 10000 post limit #
def deep_scrape(stocklist, sublist, num_workers, max_posts):
    assert isinstance(stocklist, list) and all(isinstance(stock, str) for stock in stocklist), "stocklist must be a list of strings"
    assert isinstance(sublist, list) and all(isinstance(sub, str) for sub in sublist), "sublist must be a list of strings"
    assert isinstance(num_workers, int) and num_workers > 0, "num_workers must be a positive integer"
    assert isinstance(max_posts, int) and max_posts > 0, "max_posts must be a positive integer"
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

    executor = ThreadPoolExecutor(max_workers=num_workers)
    threads = list()
    for x in range(len(subreddits)):
        sub = subreddits[x]

        threads.append(
            executor.submit(SubredditScraper(sub, lim=max_posts, sort='new').get_posts, stocklist))
        threads.append(
            executor.submit(SubredditScraper(sub, lim=max_posts, sort='hot').get_posts, stocklist))
        threads.append(
            executor.submit(SubredditScraper(sub, lim=max_posts, sort='top').get_posts, stocklist))
        print(f"Starting threads: {(x*3)+1}-{(x*3)+3}")

    for thread in threads:
        thread.result(timeout=constant.SECONDS_IN_DAY)
        print(f"Thread {threads.index(thread) + 1} Joined")


if __name__ == '__main__':
    # CONFIG #
    user = DatabaseHandler.User("my_win")
    num_threads = 12
    num_posts = 10000

    # stocklist options:
    #    ["stocks"]         //all stocks on NYSE and Nasdaq
    #    ["crypto"]         //all crypto
    #    ["abc", "def"]     //custom
    symbols = ["PTON"]

    # sublist options:
    #    ["all"]         //crypto and stock subs
    #    ["stocks"]      //stock subs
    #    ["crypto"]      //crypto subs
    #    ["abc", "def"]  //custom
    subs = ["wallstreetbets"]

    start_time = time.time()
    deep_scrape(symbols, subs, num_threads, num_posts)
    end_time = time.time()
    print(f"------ time: {end_time - start_time} seconds | threads: {num_threads} | posts: {num_posts}")



