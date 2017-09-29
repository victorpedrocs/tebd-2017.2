from flask import Blueprint, render_template, request, session, redirect, escape, url_for, jsonify
main = Blueprint('main', __name__)
import json
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from engine import EngineRecomendacao

from flask import Flask, request

@main.route("/")
def index():
    return render_template('index.html')

@main.route('/signin', methods=['GET', 'POST'])
def signin_page():
    if request.method == 'POST':
        session['userid'] = request.form['userid']
        return redirect(url_for('main.home_page'))
    return render_template('signin.html')

@main.route('/signup', methods=['GET', 'POST'])
def signup_page():
    if request.method == 'POST':
        user_id = request.form['userid']
        top_filmes = rec_engine.filmes_mais_populares()
        return render_template('resultado_busca.html', filmes=top_filmes, first_use=True)
    return render_template('signup.html')

@main.route('/end-signup')
def end_signup():
    rec_engine.train_model()
    return redirect(url_for('main.home_page'))


@main.route("/home")
def home_page():
    if 'userid' in session:
        user_id = session['userid']
        top_filmes = rec_engine.filmes_mais_populares()
        top_recomendados = rec_engine.melhores_recomendacoes(user_id)
        return render_template('home.html', top_filmes=top_filmes, top_recomendados=top_recomendados)
    else:
        return redirect(url_for('main.signin_page'))

@main.route('/logout')
def logout():
    session.pop('userid', None)
    return redirect(url_for('index'))

@main.route('/rate-movie/<int:id_filme>', methods=['POST'])
def rate_movies(id_filme):
    nota = request.form['nota']
    user_id = 0
    if 'userid' in session:
        user_id = session['userid']
    else:
        return redirect(url_for('main.signin_page'))
    rec_engine.avaliar_filme(user_id, id_filme, nota)

    return jsonify(data='ok')

@main.route('/busca')
def busca():
    nome_filme = request.args.get('nome', '')
    filmes = rec_engine.filmes_por_nome(nome_filme)
    return render_template('resultado_busca.html', filmes=filmes)

@main.route('/recomendation', methods=['POST'])
def recomend():
    userid = session['userid']
    filmes = []
    avaliacoes = []
    filmes.append(request.form['filme1'])
    filmes.append(request.form['filme2'])
    filmes.append(request.form['filme3'])
    filmes.append(request.form['filme4'])
    filmes.append(request.form['filme5'])
    # filmes.append(request.form['filme6'])
    # filmes.append(request.form['filme7'])
    # filmes.append(request.form['filme8'])
    # filmes.append(request.form['filme9'])
    # filmes.append(request.form['filme10'])
    avaliacoes.append(request.form['nota1'])
    avaliacoes.append(request.form['nota2'])
    avaliacoes.append(request.form['nota3'])
    avaliacoes.append(request.form['nota4'])
    avaliacoes.append(request.form['nota5'])
    # avaliacoes.append(request.form['nota6'])
    # avaliacoes.append(request.form['nota7'])
    # avaliacoes.append(request.form['nota8'])
    # avaliacoes.append(request.form['nota9'])
    # avaliacoes.append(request.form['nota10'])
    print('AVALIACOES', avaliacoes)
    print('FILMES', filmes)



def create_app(spark_context, dataset_path):
    app = Flask(__name__)
    app.register_blueprint(main)
    app.secret_key = 'A0Zr98j/3yX R~XHH!jmNLWX/,?RT'
    global rec_engine
    rec_engine = EngineRecomendacao(spark_context, dataset_path)
    return app
