
import nltk
import re, os

keyspace = 'technoaces'


from pyspark.mllib.linalg import Vector, Vectors
from pyspark.mllib.clustering import LDA, LDAModel
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
from pyspark.ml.feature import CountVectorizer , IDF

from pyspark.mllib.linalg import Vectors as MLlibVectors
from pyspark.mllib.clustering import LDA as MLlibLDA

import pyspark
from pyspark.sql import SQLContext
from nltk.corpus import stopwords
import re as re
from pyspark.ml.feature import CountVectorizer , IDF

from pyspark.mllib.linalg import Vector, Vectors
from pyspark.mllib.clustering import LDA, LDAModel

cluster_seeds = ['127.0.0.1']
spark = SparkSession.builder.appName('Word Cloud').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

keyspace='technoaces'

tablenames = ['imdb_movies_data','imdb_tv_show_data','imdb_video_game_data','imdb_tv_movie_data'\
            ,'imdb_video_data','imdb_tv_special_data','imdb_tv_short_data']

def main():
    for tn in tablenames:
        data = spark.read.format("org.apache.spark.sql.cassandra")\
                    .options(table=tn, keyspace=keyspace).load().limit(1000)
        
        data = data.sort('imdb_score', ascending=False)

        desc = data.rdd.map(lambda x : x['description']).filter(lambda x: x is not None)

        StopWords = nltk.corpus.stopwords.words('english')
        StopWords.extend([" ...                See full summary"])

        tokenized = desc.map( lambda y: y.strip().lower()).map( lambda x: re.split(" ", x))\
            .map( lambda word: [x for x in word if x.isalpha()]).map( lambda word: [x for x in word if len(x) > 3] )\
            .map( lambda word: [x for x in word if x not in StopWords]).zipWithIndex()

   
        df_txts = spark.createDataFrame(tokenized, ["words",'index'])
        countVec = CountVectorizer(inputCol="words", outputCol="raw_features", vocabSize=5000, minDF=10.0)
        CountVectMod = countVec.fit(df_txts)
        result = CountVectMod.transform(df_txts)
        idf = IDF(inputCol="raw_features", outputCol="features")
        idfModel = idf.fit(result)
        resultTFIdf = idfModel.transform(result)

        totalTopics = 10
        totalItr = 100
        LDAModel = MLlibLDA.train(resultTFIdf.select('index','features').rdd.mapValues(MLlibVectors.fromML).map(list),\
                        k=totalTopics, maxIterations=totalItr)

        maxwordsTopic = 5  
        topicIndices = sc.parallelize(LDAModel.describeTopics(maxTermsPerTopic = 5))
        VCarr = CountVectMod.vocabulary

        def finalTopic(topic):
            terms = topic[0]
            result = []
            for i in range(maxwordsTopic):
                term = VCarr[terms[i]]
                result.append(term)
            return result

        topics_final = topicIndices.map(lambda topic: finalTopic(topic)).collect()
        print(topics_final)
        for topic in range(len(topics_final)):
            print ("Topic" + str(topic) + ":")
            for term in topics_final[topic]:
                print (term)
            print ('\n')

if __name__ == '__main__':
    main()
