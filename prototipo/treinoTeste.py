import os
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS
import math
import pickle

sc = SparkContext()

avaliacoes = sc.textFile(os.path.join('..', 'datasets', 'ml-latest-small', 'ratings.csv'))
cabecalho = avaliacoes.take(1)[0]

avaliacoes = avaliacoes.filter(lambda line: line!= cabecalho)\
    .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()

treino, validacao, teste = avaliacoes.randomSplit([6, 2, 2])# RDDs
validacao_sem_notas = validacao.map(lambda x: (x[0], x[1]))#RDDs sem as notas
teste_sem_notas = teste.map(lambda x: (x[0], x[1]))

iteracoes = 10
regularizacao = 0.1 # melhora um pouco
fatores_latentes = [4, 8, 12]#testar com outros
erros = []

min_erro = float('inf')
fator_latente_escolhido = -1
best_iteration = -1

for fator_latente in fatores_latentes:
    
    model = ALS.train(treino, fator_latente, iterations=iteracoes, lambda_=regularizacao)
    predicoes = model.predictAll(validacao_sem_notas).map(lambda r: ((r[0], r[1]), r[2]))
    rates_and_preds = validacao.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predicoes)
    erro = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
    erros.append(erro)
        
    print ('Com fator latente %s o RMSE foi de %s' % (fator_latente, erro))
    
    if erro < min_erro:
        min_erro = erro
        fator_latente_escolhido = fator_latente

print ('O fator latente com menor erro foi: %s' % fator_latente_escolhido)

pickle.dump(fator_latente_escolhido, open("/home/elaine/workspace/TrabFinalTEBDI/fator_latente_escolhido.txt", "wb"))

model = ALS.train(treino, fator_latente_escolhido, iterations=iteracoes, lambda_=regularizacao)
predicoes = model.predictAll(teste_sem_notas).map(lambda r: ((r[0], r[1]), r[2]))
rates_and_preds = teste.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predicoes)
erro = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
    
print ('Para o conjunto de teste o RMSE foi de: %s' % (erro))

