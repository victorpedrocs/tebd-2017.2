import time, sys, cherrypy, os
from paste.translogger import TransLogger
from app import create_app
from pyspark import SparkContext, SparkConf

def init_spark_context():
    conf = SparkConf().setAppName("cf_spark-server")
    context = SparkContext(conf=conf, pyFiles=['app.py', 'engine.py'])
    context.setLogLevel('ERROR')

    return context

def run_server(app):
    app_logged = TransLogger(app)

    cherrypy.tree.graft(app_logged, '/')

    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 8080,
        'server.socket_host': '0.0.0.0'
    })

    cherrypy.engine.start()
    cherrypy.engine.block()

if __name__ == "__main__":
    context = init_spark_context()
    dataset_path = os.path.join('datasets', 'ml-latest')
    app = create_app(context, dataset_path)

    run_server(app)

