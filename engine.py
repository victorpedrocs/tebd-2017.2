import os
import pickle
import math
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS

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

  def __train_model(self):
    logger.info("Treinando modelo...")
    self.model = ALS.train(self.avaliacoes_rdd, self.fator_latente_escolhido,
                            iterations=self.iteracoes, lambda_=self.regularizacao)
    logger.info("Modelo Treinado")
    # pickle.dump(self.fator_latente_escolhido, open("./fator_latente_escolhido.txt", "wb"))

  def get_top_ratings(self, user_id, n_filmes, id_filmes, notas):
    if id_usuario <= 71567:
      novas_avaliacoes = []

      for i in range (len(id_filmes)):
        novas_avaliacoes.append((id_usuario, id_filmes[i], notas[i]))
    
      # id_filme = map(lambda x: x[1], novas_avaliacoes)
      filmes_nao_avaliados = (self.filmes.filter(lambda x: x[0] not in id_filmes).map(lambda x: (id_usuario, x[0])))
      recomendacao = model.predictAll(filmes_nao_avaliados)
      
      id_filme_notas_previstas = recomendacao.map(lambda x: (x.product, x.rating)).join(titulo_filme).join(av_count_filme)    
      titulo_reviews = id_filme_notas_previstas.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))    
      filmes_recomendados = titulo_reviews.filter(lambda r: r[2]>=20).takeOrdered(20, key=lambda x: -x[1])
      
      print ('Lista de filmes:\n%s' % '\n'.join(map(str, filmes_recomendados)))
    
    else: 
    
      novas_avaliacoes = [] 
      numero_avaliacoes = 2
      
      for i in range (numero_avaliacoes):   
        
        movieID = int(input('Id do filme : '))
        rating = int(input('Avaliacao : '))
        novas_avaliacoes.append((id_usuario, movieID, rating))
        
      pickle.dump(novas_avaliacoes, open("./novas_avaliacoes.txt", "wb"))
      
      id_filme = map(lambda x: x[1], novas_avaliacoes)
      filmes_nao_avaliados = (filmes.filter(lambda x: x[0] not in id_filme).map(lambda x: (id_usuario, x[0])))
      mais_populares = av_count_filme.map(lambda x: (x[0], x[2]))
    
      #antes id movie, notas previstas, agora id movie e media das notas
      
      id_filme_notas_previstas = mais_populares.join(titulo_filme).join(av_count_filme)    
      titulo_reviews = id_filme_notas_previstas.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))    
      filmes_recomendados = titulo_reviews.filter(lambda r: r[2]>=20).takeOrdered(20, key=lambda x: -x[1])
      
      print ('Lista de filmes:\n%s' % '\n'.join(map(str, filmes_recomendados)))



  
  def filmes_por_nome(self, nome_filme):
    print(nome_filme)
    return self.filmes_rdd.filter(lambda line: nome_filme.lower() in line[1].lower()).map(lambda l: l[1]).collect()
  
  def filmes_mais_populares(self):
    logger.info('Agrupando filmes e notas pelo id do filme')
    id_av_filme = (self.avaliacoes_rdd.map(lambda x: (int(x[1]), float(x[2]))).groupByKey())
    logger.info('Calculando a media de notas dos filmes')
    av_count_filme = id_av_filme.map(lambda x: (x[0], len(x[1]), float(sum(x for x in x[1]))/len(x[1])))
    mais_populares = av_count_filme.map(lambda x: (x[0], x[2]))
    
    titulo_filme = self.filmes_rdd.map(lambda x: (int(x[0]),x[1]))
    logger.info('Join com rdd de filmes')
    media_notas = mais_populares.join(titulo_filme).join(av_count_filme) 
    titulo_reviews = media_notas.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))
    top_filmes = titulo_reviews.filter(lambda r: r[2]>=10).takeOrdered(10, key=lambda x: -x[1])
    return top_filmes

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
    self.filmes_rdd = self.filmes_rdd.filter(lambda line: line != cabecalho).map(lambda token: (token[0], token[1])).cache()

    # Train the model
    self.fatores_latentes = [2,4,8,16,32]
    self.menor_erro = float('inf')
    self.fator_latente_escolhido = -1
    self.iteracoes = 10
    self.regularizacao = 0.1
    self.model = None
    logger.info('Escolhendo fator latente')
    self.__escolhe_fator_latente()
    # logger.info('Treinando modelo')
    # self.__train_model()

