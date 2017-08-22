#!/usr/bin/python
# -*- coding: utf-8 -*-

from pyspark import SparkContext
import pickle
from pyspark.mllib.recommendation import MatrixFactorizationModel

sc = SparkContext()
sc.setLogLevel("ERROR")

avaliacoes = sc.textFile('/home/elaine/workspace/datasets/ml-10M100K/ratings.dat')
avaliacoes = avaliacoes.map(lambda line: line.split("::")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()

filmes = sc.textFile('/home/elaine/workspace/datasets/ml-10M100K/movies.dat')
filmes = filmes.map(lambda line: line.split("::")).map(lambda tokens: (tokens[0],tokens[1])).cache()

fator_latente_escolhido = pickle.load(open("/home/elaine/workspace/datasets/fator_latente_escolhido.txt","rb"))

titulo_filme = filmes.map(lambda x: (int(x[0]),x[1]))
id_av_filme = (avaliacoes.map(lambda x: (x[1], x[2])).groupByKey())
av_count_filme = id_av_filme.map(lambda x: (x[0], len(x[1]), float(sum(x for x in x[1]))/len(x[1])))

model = MatrixFactorizationModel.load(sc, '/home/elaine/workspace/datasets/modelo_als')

'''----Usuario da base com novas avaliacoes-----'''

id_usuario = int(input('Id do usuario : '))

if id_usuario <= 71567:
    
    novas_avaliacoes = [] 
    numero_avaliacoes = 10
    
    for i in range (numero_avaliacoes):   
        
        movieID = int(input('Id do filme : '))
        rating = int(input('Avaliacao : '))
        novas_avaliacoes.append((id_usuario, movieID, rating))
    
    id_filme = map(lambda x: x[1], novas_avaliacoes)
    filmes_nao_avaliados = (filmes.filter(lambda x: x[0] not in id_filme).map(lambda x: (id_usuario, x[0])))
    recomendacao = model.predictAll(filmes_nao_avaliados)
    
    id_filme_notas_previstas = recomendacao.map(lambda x: (x.product, x.rating)).join(titulo_filme).join(av_count_filme)    
    titulo_reviews = id_filme_notas_previstas.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))    
    filmes_recomendados = titulo_reviews.filter(lambda r: r[2]>=20).takeOrdered(20, key=lambda x: -x[1])
    
    print ('Lista de filmes:\n%s' % '\n'.join(map(str, filmes_recomendados)))
    
else: 
    
    novas_avaliacoes = [] 
    numero_avaliacoes = 10
    
    for i in range (numero_avaliacoes):   
        
        movieID = int(input('Id do filme : '))
        rating = int(input('Avaliacao : '))
        novas_avaliacoes.append((id_usuario, movieID, rating))
        
    pickle.dump(novas_avaliacoes, open("/home/elaine/workspace/datasets/novas_avaliacoes.txt", "wb"))
    
    id_filme = map(lambda x: x[1], novas_avaliacoes)
    filmes_nao_avaliados = (filmes.filter(lambda x: x[0] not in id_filme).map(lambda x: (id_usuario, x[0])))
    mais_populares = av_count_filme.map(lambda x: (x[0], x[2]))
    
    #antes id movie, notas previstas, agora id movie e media das notas
    
    id_filme_notas_previstas = mais_populares.join(titulo_filme).join(av_count_filme)    
    titulo_reviews = id_filme_notas_previstas.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))    
    filmes_recomendados = titulo_reviews.filter(lambda r: r[2]>=20).takeOrdered(20, key=lambda x: -x[1])
    
    print ('Lista de filmes:\n%s' % '\n'.join(map(str, filmes_recomendados)))  
    

    
    
    
