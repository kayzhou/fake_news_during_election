from flask import Flask
from flask import render_template
app = Flask(__name__)


@app.route('/')
def index():
    return render_template("my-echats.html")


@app.route('/<file_name>')
def get_file(file_name):
    return render_template(file_name) 


if __name__ == '__main__':
    app.run(debug=True)