#!/usr/bin/python
# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS
import pickle
import math

sc = SparkContext()
sc.setLogLevel("ERROR")

avaliacoes = sc.textFile('/home/elaine/workspace/datasets/ml-10M100K/ratings.dat')
avaliacoes = avaliacoes.map(lambda line: line.split("::")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()

treino, teste,novas_av = avaliacoes.randomSplit([7,2,1])

novo_usuario = pickle.load(open("/home/elaine/workspace/datasets/novas_avaliacoes.txt","rb"))
novas_avaliacoes_usuarios = sc.parallelize(novo_usuario)

fator_latente_escolhido = pickle.load(open("/home/elaine/workspace/datasets/fator_latente_escolhido.txt","rb"))

novas_avaliacoes = treino.union(novas_av).union(novas_avaliacoes_usuarios)

#em batches
model = ALS.train(novas_avaliacoes, fator_latente_escolhido, iterations=10, lambda_=0.1)
teste_sem_notas = novas_av.map(lambda x: (x[0], x[1]))

predicoes = model.predictAll(teste_sem_notas).map(lambda r: ((r[0], r[1]), r[2]))
rates_and_preds = novas_av.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predicoes)
erro = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())

print ('Erro no conjunto de teste: %s' % (erro))
