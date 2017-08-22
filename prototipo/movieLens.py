import os
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS
import pickle

sc = SparkContext()

avaliacoes = sc.textFile(os.path.join('..', 'datasets', 'ml-latest-small', 'ratings.csv'))
cabecalho = avaliacoes.take(1)[0]

avaliacoes = avaliacoes.filter(lambda line: line!= cabecalho)\
    .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()

filmes = sc.textFile(os.path.join('..', 'datasets', 'ml-latest-small', 'movies.csv'))
cabecalho_filme = filmes.take(1)[0]

filmes = filmes.filter(lambda line: line!= cabecalho_filme)\
    .map(lambda line: line.split(",")).map(lambda tokens: (tokens[0],tokens[1])).cache()

fator_latente_escolhido = pickle.load(open("/home/elaine/workspace/TrabFinalTEBDI/fator_latente_escolhido.txt","rb"))

iterations = 10
regularization_parameter = 0.1 # mesmo que pro outro modelo

movies_titles = filmes.map(lambda x: (int(x[0]),x[1]))

#print ("There are %s movies in the dataset" % (movies_titles.count()))

movie_ID_with_ratings_RDD = (avaliacoes.map(lambda x: (x[1], x[2])).groupByKey())
movie_rating_counts_RDD = movie_ID_with_ratings_RDD.map(lambda x: (x[0], len(x[1])))#agora so tem as avaliacoes

new_user_ID = int(input('Id do usuario : '))
new_user_ratings = []
numero_avaliacoes = 2

for i in range (numero_avaliacoes):   
    
    movieID = int(input('Id do filme : '))
    rating = int(input('Avaliacao : '))
    new_user_ratings.append((new_user_ID, movieID, rating))

new_user_ratings_RDD = sc.parallelize(new_user_ratings)

print ('New user ratings: %s' % new_user_ratings_RDD.take(10))

data_with_new_ratings_RDD = avaliacoes.union(new_user_ratings_RDD)

new_ratings_model = ALS.train(data_with_new_ratings_RDD, fator_latente_escolhido, iterations=iterations, lambda_=regularization_parameter)

new_user_ratings_ids = map(lambda x: x[1], new_user_ratings) # get just movie IDs

new_user_unrated_movies_RDD = (filmes.filter(lambda x: x[0] not in new_user_ratings_ids).map(lambda x: (new_user_ID, x[0])))

new_user_recommendations_RDD = new_ratings_model.predictAll(new_user_unrated_movies_RDD)

# Transform new_user_recommendations_RDD into pairs of the form (Movie ID, Predicted Rating)
new_user_recommendations_rating_RDD = new_user_recommendations_RDD.map(lambda x: (x.product, x.rating))
new_user_recommendations_rating_title_and_count_RDD = \
    new_user_recommendations_rating_RDD.join(movies_titles).join(movie_rating_counts_RDD)
    
new_user_recommendations_rating_title_and_count_RDD = \
    new_user_recommendations_rating_title_and_count_RDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))
    
top_movies = new_user_recommendations_rating_title_and_count_RDD.filter(lambda r: r[2]>=25).takeOrdered(25, key=lambda x: -x[1])

print ('TOP recommended movies (with more than 25 reviews):\n%s' %
        '\n'.join(map(str, top_movies)))
