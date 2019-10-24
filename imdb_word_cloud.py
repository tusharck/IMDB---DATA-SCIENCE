
import matplotlib.pyplot as plt
import nltk
from pyspark.ml.feature import *
from pyspark.sql import SparkSession, functions, types
from wordcloud import WordCloud
import re, operator, string
from pyspark.ml.feature import StopWordsRemover
from textblob import Word
nltk.download('wordnet')

def clean(line):
    for x in line[0]:
        Wsep = re.compile(r'[%s]+' % re.escape(string.punctuation))
        s = Wsep.split(x)
        for w in s:
            if (len(w)>1):
                yield (Word(w).lemmatize(), 1)

cluster_seeds = ['127.0.0.1']
spark = SparkSession.builder.appName('Word Cloud').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

keyspace='technoaces'

tablenames = ['imdb_movies_data','imdb_tv_show_data','imdb_video_game_data','imdb_tv_movie_data'\
            ,'imdb_video_data','imdb_tv_special_data','imdb_tv_short_data']

swords=['na','short']

def main():
    stopWordsRemover = stopWordsRemover(inputCol="wcwords", outputCol="final",stopWords=swords)
    tokenizer = Tokenizer(inputCol="genre", outputCol="wcwords")

    for tn in tablenames:
        data = spark.read.format("org.apache.spark.sql.cassandra")\
                    .options(table=tn, keyspace=keyspace).load().select("genre")
        tokenData = tokenizer.transform(data)

        afterSWRem = stopWordsRemover.transform(tokenData).select("final")
        freqData = afterSWRem.rdd.flatMap(clean).reduceByKey(operator.add)\
                        .sortBy(lambda t: t[1], ascending = False).collect()
        dataDict = dict(freqData)

        IMDBwc = WordCloud(width =1600,height=800, background_color="white", max_words=100)\
                            .generate_from_frequencies(dataDict)
        plt.figure(figsize=[80,40])
        plt.imshow(IMDBwc)
        plt.axis('off')
        plt.savefig('wcop/' + tn + '.png', format="png",pad_inches=0)

if __name__ == '__main__':
    main()
