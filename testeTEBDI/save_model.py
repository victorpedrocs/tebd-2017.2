#!/usr/bin/python
# -*- coding: utf-8 -*-
import pickle
import math
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS

sc = SparkContext()
sc.setLogLevel("ERROR")

avaliacoes = sc.textFile('/home/elaine/workspace/datasets/ml-10M100K/ratings.dat')
avaliacoes = avaliacoes.map(lambda line: line.split("::")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()

fator_latente_escolhido = pickle.load(open("/home/elaine/workspace/datasets/fator_latente_escolhido.txt","rb"))

'''acrescentando novos usuarios com novas avaliacoes = novas_av'''
treino, teste,novas_av = avaliacoes.randomSplit([7,2,1])
model = ALS.train(treino, fator_latente_escolhido,iterations=10, lambda_=0.1)
teste_sem_notas = teste.map(lambda x: (x[0], x[1]))

predicoes = model.predictAll(teste_sem_notas).map(lambda r: ((r[0], r[1]), r[2]))
rates_and_preds = teste.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predicoes)
erro = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())

model.save(sc, '/home/elaine/workspace/datasets/modelo_als')

print ('Erro no conjunto de teste: %s' % (erro))