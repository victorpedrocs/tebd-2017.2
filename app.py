from flask import Blueprint, render_template
main = Blueprint('main', __name__)
import json
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request

@main.route("/")
def homepage():
    return render_template('home.html')

@main.route("/signin")
def signin_page():
    return render_template('signin.html')

def create_app(spark_context, dataset_path):
    app = Flask(__name__)
    app.register_blueprint(main)
    return app
