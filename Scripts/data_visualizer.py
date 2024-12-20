import collections

from matplotlib import pyplot as plt

import DatabaseHandler


# 'r_' indicates reddit attribute
# 'y_' indicates youtube attribute
class StockHist(object):
    def __init__(self, symbol, r_votes, r_comments, r_dates, y_views, y_dates):
        self.symbol = symbol
        self.r_votes = r_votes
        self.r_comments = r_comments
        self.r_dates = r_dates
        self.y_views = y_views
        self.y_dates = y_dates


def get_hist(symbol, date_range):
    assert isinstance(symbol, str) and symbol, "Symbol must be a non-empty string"
    assert isinstance(date_range, tuple) and len(date_range) == 2, "Date range must be a tuple with two elements"
    # get reddit history #
    cnx = DatabaseHandler.connect_to_db(user, "TheSpatula")
    mycursor = cnx.cursor()
    assert mycursor
    assert DatabaseHandler.table_exists(mycursor, "reddit")

    mycursor.execute(f"""
        SELECT num_votes, num_comments, DATE(date_posted)
        FROM reddit
        WHERE symbol="{symbol}"
        AND date_posted > "{date_range[0]}"
        AND date_posted < "{date_range[1]}"
        ;
        """)

    r_dict = collections.OrderedDict()
    # r_dict = {"date":[votes, comments]}

    for (num_votes, num_comments, date_posted) in mycursor:
        if date_posted in r_dict.keys():
            r_dict[date_posted][0] += num_votes
            r_dict[date_posted][1] += num_comments

        else:
            r_dict[date_posted] = [num_votes, num_comments]

    mycursor.close()
    cnx.close()

    r_dict = collections.OrderedDict(sorted(r_dict.items()))

    # get youtube history #
    cnx = DatabaseHandler.connect_to_db(user, "TheSpatula")
    mycursor = cnx.cursor()
    assert mycursor
    assert DatabaseHandler.table_exists(mycursor, "youtube")

    mycursor.execute(f"""
        SELECT num_views, date_posted
        FROM youtube
        WHERE symbol="{symbol}"
        AND date_posted > "{date_range[0]}"
        AND date_posted < "{date_range[1]}"
        ;
        """)

    y_dict = collections.OrderedDict()
    # y_dict = {"date" : views} #

    for (num_views, date_posted) in mycursor:
        if date_posted in y_dict.keys():
            y_dict[date_posted] += num_views / 100
        else:
            y_dict[date_posted] = num_views / 100

    mycursor.close()
    cnx.close()

    y_dict = collections.OrderedDict(sorted(y_dict.items()))

    r_votes = [vals[0] for vals in r_dict.values()]
    r_comments = [vals[1] for vals in r_dict.values()]
    r_dates = r_dict.keys()
    y_views = y_dict.values()
    y_dates = y_dict.keys()

    return StockHist(symbol, r_votes, r_comments, r_dates, y_views, y_dates)


def plot_hist(symbol, data_date_range, chart_reddit, chart_youtube):
    assert isinstance(symbol, str) and symbol, "Symbol must be a non-empty string"
    assert isinstance(data_date_range, tuple) and len(data_date_range) == 2, "Date range must be a tuple with two elements"
    assert isinstance(chart_reddit, bool), "chart_reddit must be a boolean"
    assert isinstance(chart_youtube, bool), "chart_youtube must be a boolean"

    hist = get_hist(symbol, data_date_range)

    if chart_reddit:
        plt.plot(hist.r_dates, hist.r_votes, marker='o')
        plt.plot(hist.r_dates, hist.r_comments, marker='o')
    if chart_youtube:
        plt.plot(hist.y_dates, hist.y_views, marker='o')  # youtube

    plt.title(f"{symbol} Chart")
    plt.xlabel("Date Posted")
    plt.ylabel("Units")
    plt.legend(["Reddit Votes", "Reddit Comments", "Youtube Views [Hundreds]"])

    fig = plt.gcf()
    fig.set_size_inches(18.5, 10.5)
    plt.show()


if __name__ == "__main__":
    user = DatabaseHandler.User("my_win")

    timeframe = ("2022-02-01", "2022-02-08")

    plot_hist("PTON", timeframe, chart_reddit=True, chart_youtube=False)
