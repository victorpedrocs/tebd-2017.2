import os
import pickle
import math
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel
import shutil

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EngineRecomendacao:

  def __escolhe_fator_latente(self):
    dataset_test = self.sc.textFile('./ml-1m/ratings.dat')
    dataset_test = dataset_test.map(lambda line: line.split("::")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()

    treino, teste = self.avaliacoes_rdd.randomSplit([8,2])
    teste_sem_notas = teste.map(lambda x: (x[0], x[1]))
    menor_erro = float('inf')

    for fator_latente in self.fatores_latentes:
      model = ALS.train(treino, fator_latente, iterations=self.iteracoes, lambda_=self.regularizacao)
      predicoes = model.predictAll(teste_sem_notas).map(lambda r: ((r[0], r[1]), r[2]))
      rates_and_preds = teste.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predicoes)
      erro = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
 
      if erro < menor_erro:
        menor_erro = erro
        self.fator_latente_escolhido = fator_latente

    pickle.dump(self.fator_latente_escolhido, open("datasets/fator_latente_escolhido.txt", "wb"))

  def get_fator_latente_from_disk(self):
    self.fator_latente_escolhido = pickle.load(open("./datasets/fator_latente_escolhido.txt","rb"))

  def calcular_contagem_avaliacoes(self):
    logger.info('Agrupando filmes e notas pelo id do filme')
    id_av_filme = (self.avaliacoes_rdd.map(lambda x: (int(x[1]), float(x[2]))).groupByKey())
    logger.info('Calculando a media de notas dos filmes')
    self.av_count_filme = id_av_filme.map(lambda x: (x[0], len(x[1]), float(sum(x for x in x[1]))/len(x[1])))

  def train_model(self):
    
    logger.info("Treinando modelo...")
    self.model = ALS.train(self.avaliacoes_rdd, self.fator_latente_escolhido,
                            iterations=self.iteracoes, lambda_=self.regularizacao)

    # Apaga pasta com modelo antigo
    shutil.rmtree('./datasets/modelo_als')
    # Salva o novo modelo
    self.model.save(self.sc, './datasets/modelo_als')
    
    logger.info("Modelo Treinado")

  def load_model(self):
    logger.info(" Load model from file")
    self.model = MatrixFactorizationModel.load(self.sc, './datasets/modelo_als')


  def filmes_por_nome(self, nome_filme):
    return self.filmes_rdd.filter(lambda line: nome_filme.lower() in line[1].lower()).collect()

  
  def filmes_mais_populares(self):
    logger.info(' Recuperando filmes mais populares')
    mais_populares = self.av_count_filme.map(lambda x: (x[0], x[2]))
    
    titulo_filme = self.filmes_rdd.map(lambda x: (int(x[0]),x[1]))
    
    media_notas = mais_populares.join(titulo_filme).join(self.av_count_filme) 
    titulo_reviews = media_notas.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))
    top_filmes = titulo_reviews.filter(lambda r: r[2]>=10).takeOrdered(10, key=lambda x: -x[1])
    return top_filmes

  def avaliar_filme(self,id_usuario, id_filme, nota):
    avaliacao = [(id_usuario, id_filme, nota)]
    nova_avaliacao_rdd = self.sc.parallelize(avaliacao)
    self.avaliacoes_rdd = self.avaliacoes_rdd.filter(lambda a: a[0] != id_usuario and ap[1] != id_filme)
    self.avaliacoes_rdd.union(nova_avaliacao_rdd)

  def melhores_recomendacoes(self, id_usuario):
    logger.info(' Recuperando recomendacoes para o usuario')
    # recuperar os filmes não avaliados pelo usuario
    filmes_nao_avaliados = self.avaliacoes_rdd.filter(lambda a: a[0] != id_usuario).map(lambda m: m[1]).countByValue()
    fna_rdd = self.sc.parallelize(filmes_nao_avaliados.keys())
    fna_rdd = fna_rdd.map(lambda l: (id_usuario, l))

    # recuperar as sugestoes para o usuario
    sugestoes_rdd = self.model.predictAll(fna_rdd)
    
    titulo_filme = self.filmes_rdd.map(lambda x: (int(x[0]),x[1]))

    filmes_sugeridos = sugestoes_rdd.map(lambda l: (l.product, l.rating)).join(titulo_filme).join(self.av_count_filme)
    
    titulo_reviews = filmes_sugeridos.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))
    filmes_recomendados = titulo_reviews.filter(lambda r: r[2]>=20).takeOrdered(10, key=lambda x: -x[1])

    return filmes_recomendados

  def __init__(self, sc, dataset_path):

    logger.info("Iniciando engine de recomendacao: ")
    self.sc = sc
    logger.info("Carregando avaliacoes...")
    self.avaliacoes_rdd = sc.textFile('datasets/ml-latest-small/ratings.csv').map(lambda line: line.split(','))
    cabecalho = self.avaliacoes_rdd.take(1)[0]
    self.avaliacoes_rdd = self.avaliacoes_rdd.filter(lambda line: line != cabecalho).map(lambda tokens: ( int(tokens[0]) , int(tokens[1]) , float(tokens[2]) )).cache()

    logger.info("Carregando filmes...")
    self.filmes_rdd = sc.textFile('datasets/ml-latest-small/movies.csv').map(lambda line: line.split(','))
    cabecalho = self.filmes_rdd.take(1)[0]
    self.filmes_rdd = self.filmes_rdd.filter(lambda line: line != cabecalho).map(lambda token: (token[0], token[1], token[2])).cache()

    # Train the model
    self.fatores_latentes = [2,4,8,16,32]
    self.menor_erro = float('inf')
    self.fator_latente_escolhido = -1
    self.iteracoes = 10
    self.regularizacao = 0.1
    self.model = None
    self.av_count_filme = 0



    self.calcular_contagem_avaliacoes()
    self.get_fator_latente_from_disk()
    self.load_model()
    # logger.info('Escolhendo fator latente')
    # self.__escolhe_fator_latente()
    # logger.info('Treinando modelo')
    # self.train_model()

