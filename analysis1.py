import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import nltk
from pyspark.ml.feature import *
from pyspark.sql import SparkSession, functions, types
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
#cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('Analysis Part 1').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

keyspace='technoaces'

def main():
    swords=['na','short']

    stopWordsRemover = StopWordsRemover(inputCol="wcwords", outputCol="final",stopWords=swords)
    tokenizer = Tokenizer(inputCol="genre", outputCol="wcwords")


    tablenames = ['imdb_movies_data','imdb_tv_show_data','imdb_video_game_data','imdb_tv_movie_data'\
                ,'imdb_video_data','imdb_tv_special_data','imdb_tv_short_data']

    for tn in tablenames:
        data = spark.read.format("org.apache.spark.sql.cassandra")\
                .options(table=tn, keyspace=keyspace).load().cache()
        check_freq = data.select('genre')
        tokenData = tokenizer.transform(check_freq)
        afterSWRem = stopWordsRemover.transform(tokenData).select("final")
        freqData = afterSWRem.rdd.flatMap(clean).reduceByKey(operator.add)\
                        .sortBy(lambda t: t[1], ascending = False).take(3)
        if tn == 'imdb_tv_show_data':
            for i in range(len(freqData)):
                data = data.where(functions.lower(data['genre']).contains(str(freqData[i][0])))
            
                avgRatingbyyear = data.where(data['votes']>0).groupby('start_year').agg(functions.avg('votes')\
                                    .alias('avgvotes')).sort('start_year')
                final = avgRatingbyyear.toPandas()

                plt.bar(final.start_year,final.avgvotes,color='orange')
                plt.ylabel('Votes')
                plt.xlabel('Start Year')
                plt.xlim((2000,2018))
                
                plt.savefig('IMDBscoreAnalysisOP/' + tn + str(freqData[i][0]) + '.png', format="png")
        else:
            for i in range(len(freqData)):
                data = data.where(functions.lower(data['genre']).contains(str(freqData[i][0])))
                
                avgRatingbyyear = data.where(data['votes']>0).groupby('year').agg(functions.avg('votes').alias('avgvotes')).sort('year')
                avgRatingbyyear.show()
                final = avgRatingbyyear.toPandas()

                plt.bar(final.year,final.avgvotes,color='orange')
                plt.xlim((2000,2018))
                plt.ylabel('Votes')
                plt.xlabel('Year')
                
                plt.savefig('IMDBscoreAnalysisOP/' + tn +'_'+ str(freqData[i][0]) + '.png', format="png")
        
if __name__ == '__main__':
    main()
