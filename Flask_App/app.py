from flask import Flask, render_template
from Scripts import DatabaseHandler

app = Flask(__name__)


@app.route("/")
def index():
	user = DatabaseHandler.User("my_win")
	cnx = DatabaseHandler.connect_to_db(user, "TheSpatula")
	mycursor = cnx.cursor()
	assert mycursor
	assert DatabaseHandler.table_exists(mycursor, "crypto_symbols")
	mycursor.execute(f"""SELECT symbol FROM crypto_symbols;""")
	ticker_list = list()
	for row in mycursor:
		ticker_list.append(row[0])

	return render_template("index.html", ticker_list=ticker_list)
