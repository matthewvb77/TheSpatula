from flask import Flask, render_template, request

app = Flask(__name__)


@app.route("/")
def index():
    name = request.args.get("name", "default")
    return render_template("index.html", name=name)


@app.route("/register", methods=["POST"])
def register():
    name = request.form.get("name")
    dorm = request.form.get("dorm")
    if not name or not dorm:
        return "failure"
    return render_template("success.html")
