from flask import Flask, render_template, request

app = Flask(__name__)

@app.route("/")
def index():
	name = request.args.get("name", "default")
	return render_template("tut2.html", name=name)

@app.route("/register", methods=["POST"])
def register():
	
