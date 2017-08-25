#!/usr/bin/python
# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS
import math
import pickle

sc = SparkContext()
sc.setLogLevel("ERROR")

avaliacoes = sc.textFile('/Users/vpedro/PESC/2/tebd/assignments/trabalho/ml-latest-small/ratings.csv')
header = avaliacoes.take(1)[0]

avaliacoes = avaliacoes.filter(lambda line: line != header).map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()

treino, teste = avaliacoes.randomSplit([8,2])
teste_sem_notas = teste.map(lambda x: (x[0], x[1]))

fatores_latentes = [2,4,8,16,32]
menor_erro = float('inf')
fator_latente_escolhido = -1

for fator_latente in fatores_latentes:
    
    model = ALS.train(treino, fator_latente, iterations=10, lambda_=0.1)
    predicoes = model.predictAll(teste_sem_notas).map(lambda r: ((r[0], r[1]), r[2]))
    rates_and_preds = teste.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predicoes)
    erro = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
        
    if erro < menor_erro:
        menor_erro = erro
        fator_latente_escolhido = fator_latente

pickle.dump(fator_latente_escolhido, open("./fator_latente_escolhido.txt", "wb"))

print(fator_latente_escolhido)
print(menor_erro)

