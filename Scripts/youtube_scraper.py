import json
import sys
import threading
import time
from datetime import datetime
from datetime import timedelta

import mysql.connector
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from tqdm import tqdm

# GLOBAL VARIABLES
path = str()
db_user = str()
db_host = str()
db_password = str()


# Sets parameters according to machine #
def config(machine):
    global path
    global db_user
    global db_host
    global db_password

    if machine == 'mac':
        path = '/opt/homebrew/bin/chromedriver'
        with open('mac_creds.json') as f:
            data = json.load(f)
            db_host = data['hostW']
            db_user = data['user']
            db_password = data['password']

    elif machine == 'win':
        path = 'C:/WebDriver/chromedriver'
        db_user = 'root'
        db_host = 'localhost'
        with open('win_creds.json') as f:
            data = json.load(f)
            db_password = data['password']

    else:
        sys.exit(f"Machine \"{machine}\" not recognized")


class StockVid(object):
    def __init__(self, href, date, views):
        self.post_id = href
        self.date = date
        self.views = views

    def json_enc(self):
        return {'stock': self.stock, 'date': self.date, 'views': self.views}


def json_def_encoder(obj):
    if hasattr(obj, 'json_enc'):
        return obj.json_enc()
    else:  # some default behavior
        return obj.__dict__


# DATABASE FUNCTIONS #

# returns connection object #
def connect_to_db(db_name):
    cnx = mysql.connector.connect(
        user='root',
        password='chalkHorseMountain',
        host='localhost',
        database=db_name
    )
    return cnx

    # returns boolean #


def table_exists(cursor, tbl_name):
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
        AND table_name = "{tbl_name}";
    """)

    if cursor.fetchone()[0] == 1:
        return True
    return False


def get_date(post):
    today = datetime.today()

    try:
        posted = post.find_element_by_xpath(".//*[@id='metadata-line']/span[2]").text

        if "just now" in posted:
            return today.strftime('%Y-%m-%d')
    except:
        # If live, return today
        return today.strftime('%Y-%m-%d')

    # Example: vid_time_data = [5, "hours", "ago"]
    vid_time_data = posted.split()

    # Remove "Streamed"
    if vid_time_data[0] == "Streamed":
        vid_time_data = vid_time_data[1:]

    if "minute" in posted:
        return (today - timedelta(minutes=int(vid_time_data[0]))).strftime('%Y-%m-%d')

    if "hour" in posted:
        return (today - timedelta(hours=int(vid_time_data[0]))).strftime('%Y-%m-%d')

    if "day" in posted:
        return (today - timedelta(days=int(vid_time_data[0]))).strftime('%Y-%m-%d')

    if "week" in posted:
        return (today - timedelta(days=7 * int(vid_time_data[0]))).strftime('%Y-%m-%d')

    if "month" in posted:
        return (today - timedelta(days=30 * int(vid_time_data[0]))).strftime('%Y-%m-%d')

    if "year" in posted:
        return (today - timedelta(days=365 * int(vid_time_data[0]))).strftime('%Y-%m-%d')

    raise Exception(f"Invalid post time-metadata: {vid_time_data}")


def get_views(post):
    try:
        views_data = post.find_element_by_xpath(".//*[@id='metadata-line']/span[1]").text
    except:
        # Skip if important data connot be found. #
        return None

    views_data = views_data.split()
    views_data = views_data[0]

    if views_data[-1] == "K":
        num_views = int(float(views_data[:-1]) * 1000)

    elif views_data[-1] == "M":
        num_views = int(float(views_data[:-1]) * 1000000)

    elif views_data[-1] == "B":
        num_views = int(float(views_data[:-1]) * 1000000000)

    elif views_data == "No":
        num_views = 0

    elif views_data == "Scheduled" or views_data == "Premieres":
        num_views = None

    else:
        num_views = int(views_data)

    return num_views


class YoutubeScraper:

    def get_vids(self, stock):

        stock_tickers = {stock: {}}
        relevant_posts = []

        # Use Headless browser
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        driver = webdriver.Chrome(path, options=options)
        driver.get("https://www.youtube.com/results?search_query=" + stock)

        actions = ActionChains(driver)

        # Begin Scrolling #
        start = time.perf_counter()
        print("[1/3] Scrolling To Bottom", end=' ')

        while True:
            try:
                driver.find_element_by_xpath("//*[@id='message'][text()='No more results']")
                break
            except:
                height = driver.execute_script("return document.body.scrollHeight")
                time.sleep(1)
                driver.find_element_by_tag_name('body').send_keys(Keys.END)
                continue

            break

        end = time.perf_counter()
        p1_time = divmod(int(end - start), 60)
        print("[{:02}:{:02}]".format(p1_time[0], p1_time[1]))

        # Begin Scraping #

        all_posts = driver.find_elements_by_xpath("//div[@ID='contents']/ytd-video-renderer")

        for i in tqdm(range(len(all_posts)), desc=f'[2/3] Scraping {stock} Vids'):
            post = all_posts[i]

            url = post.find_element_by_xpath(".//*[@id='video-title']").get_attribute("href")
            # The last 11 chars of the url is the unique key
            post_id = url[-11:]
            num_views = get_views(post)

            if num_views is None:  # number of views was not listed (ex. TV show that you need to buy)
                continue
            post_date = get_date(post)

            stock_tickers[stock][i] = StockVid(post_id, post_date, num_views)

        driver.close()

        # Begin Formatting Results #
        if len(stock_tickers[stock]) > 0:
            for post in stock_tickers[stock]:
                relevant_posts.append(stock_tickers[stock][post])
        # json_object = json.dumps(relevant_posts, default=json_def_encoder, indent = 4)
        # print(json_object)

        # Updating Database #

        cnx = connect_to_db("TheSpatula")
        mycursor = cnx.cursor()
        assert mycursor
        assert table_exists(mycursor, "youtube")

        for x in tqdm(range(len(relevant_posts)), desc="[3/3] Updating Database"):
            post = relevant_posts[x]

            # Add post, if it exists already, update post #
            mycursor.execute(f"""
            INSERT INTO youtube (post_id, symbol, num_views, date_posted) 
            VALUES("{post.post_id}", "{stock}", {post.views}, "{post.date}")
            ON DUPLICATE KEY UPDATE num_views={post.views}
            ;""")

            cnx.commit()


def deep_scrape(stocklist):
    threads = []

    for x in tqdm(range(len(stocklist)), desc="DEEP SCRAPE"):

        stock = stocklist[x]

        if stock == "":
            raise Exception(f"Empty string in stock list")

        threads.append(threading.Thread(target=YoutubeScraper().get_vids, args=[stock]))

        print(f"Thread {len(threads)} Starting")
        threads[-1].start()

        if stock == stocklist[-1]:
            for thread in threads:
                thread.join()
                print(f"Thread {threads.index(thread) + 1} Joined")


if __name__ == "__main__":
    config("win")

    stocklist = ["TSLA"]

    deep_scrape(stocklist)

    print("DONE!!")
