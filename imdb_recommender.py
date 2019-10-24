
import matplotlib.pyplot as plt
import nltk
from pyspark.ml.feature import *
from pyspark.sql import SparkSession, functions, types
import re, operator, string
from pyspark.ml.recommendation import ALS

cluster_seeds = ['127.0.0.1']
spark = SparkSession.builder.appName('Word Cloud').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

keyspace='technoaces'

tablenames = ['imdb_movies_data']#,'imdb_tv_show_data','imdb_video_game_data','imdb_tv_movie_data'\
            #,'imdb_video_data','imdb_tv_special_data','imdb_tv_short_data']

swords=['na','short']

def main():

    for tn in tablenames:
        data = spark.read.format("org.apache.spark.sql.cassandra")\
                    .options(table=tn, keyspace=keyspace).load().limit(1000)
        
        training, test = data.randomSplit([0.8,0.2])

        als = ALS(maxIter=5, regParam=0.01, userCol='userId', itemCol='movieId', ratingCol='rating')

        model = als.fit(training)

        predictions = model.transform(test)

if __name__ == '__main__':
    main()
