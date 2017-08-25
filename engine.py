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

    treino, teste = self.avaliacoes.randomSplit([8,2])
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

  def __train_model(self):
    logger.info("Treinando modelo...")
    self.model = ALS.train(self.avaliacoes, self.fator_latente_escolhido,
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



  def filmes_por_nome(nome_fime):
    return self.filmesRDD.filter(lambda line: nome_filme.lower() in line[1].lower()).collect()

  def __init__(self, sc, dataset_path):

    logger.info("Iniciando engine de recomendacao: ")

    self.sc = sc

    # carregar avaliacoes
    logger.info("Carregando avaliacoes...")

    self.avaliacoes = sc.textFile('./ml-10M100K/ratings.dat')
    self.avaliacoes = self.avaliacoes.map(lambda line: line.split("::")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()

    # Carregar dados dos filmes
    self.filmes = sc.textFile('./ml-1m/movies.dat')
    self.filmes = self.filmes.map(lambda line: line.split("::")).map(lambda tokens: (tokens[0],tokens[1])).cache()
    self.titulos_filmes = self.filmes.map(lambda x: (int(x[0]),x[1]))
    fator_latente_escolhido = pickle.load(open("./fator_latente_escolhido.txt","rb"))

    
    id_av_filme = (self.avaliacoes.map(lambda x: (x[1], x[2])).groupByKey())
    self.av_count_filme = id_av_filme.map(lambda x: (x[0], len(x[1]), float(sum(x for x in x[1]))/len(x[1])))

    # ratings_file_path = os.path.join(dataset_path, 'ratings.csv')
    # ratings_raw_RDD = self.sc.textFile(ratings_file_path)
    # ratings_raw_data_header = ratings_raw_RDD.take(1)[0]
    # self.ratings_RDD = ratings_raw_RDD.filter(lambda line: line!=ratings_raw_data_header)\
    #     .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()
    
    
    # Load movies data for later use
    # logger.info("Loading Movies data...")
    # movies_file_path = os.path.join(dataset_path, 'movies.csv')
    # movies_raw_RDD = self.sc.textFile(movies_file_path)
    # movies_raw_data_header = movies_raw_RDD.take(1)[0]
    # self.movies_RDD = movies_raw_RDD.filter(lambda line: line!=movies_raw_data_header)\
    #     .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),tokens[1],tokens[2])).cache()
    # self.movies_titles_RDD = self.movies_RDD.map(lambda x: (int(x[0]),x[1])).cache()
    # Pre-calculate movies ratings counts
    # self.__count_and_average_ratings()

    # Train the model
    self.fatores_latentes = [2,4,8,16,32]
    self.menor_erro = float('inf')
    self.fator_latente_escolhido = -1
    self.iteracoes = 10
    self.regularizacao = 0.1
    self.model = None
    self.__escolhe_fator_latente()
    self.__train_model()

