from flask import Blueprint, render_template, request, session, redirect, escape, url_for
main = Blueprint('main', __name__)
import json
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request

@main.route("/")
def index():
    return render_template('index.html')

@main.route('/signin', methods=['GET', 'POST'])
def signin_page():
    if request.method == 'POST':
        print('signin POST')
        print('userid:', request.form['userid'])
        session['userid'] = request.form['userid']
        return redirect(url_for('main.home_page'))
    return render_template('signin.html')

@main.route("/home")
def home_page():
    if 'userid' in session:
        return render_template('home.html')
    else:
        return redirect(url_for('signin_page'))

@main.route('/logout')
def logout():
    session.pop('userid', None)
    return redirect(url_for('index'))

def create_app(spark_context, dataset_path):
    app = Flask(__name__)
    app.register_blueprint(main)
    app.secret_key = 'A0Zr98j/3yX R~XHH!jmNLWX/,?RT'
    return app
