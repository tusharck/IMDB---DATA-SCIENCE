from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import lit
from pyspark.sql.functions import split

import pandas as pd
from movie_tv_tools import imdb_schema
from filter_functions import filterMovies,filter_tv,filter_games
from filter_functions import filter_tvmovie,filter_video,filter_tvspecial,filter_tvshort
import os 
import sys

cluster_seeds = ['127.0.0.1']
#cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('imdb etl').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def cleanData(line):
    line = list(line)
    y19 =line[2].find('19')
    y20 = line[2].find('20')
    if y19 > -1:
        line[2] = line[2][y19:(y19+4)]
    elif y20 > -1:
        line[2] = line[2][y20:(y20+4)]
    indexofmin = line[4].find('min')
    if indexofmin > -1:        
        line[4] = line[4].replace(',','')
        line[4] = line[4].replace(' min','')
    elif line[4] is None:
        line[4] = '0'
    else:
        line[4] ='0'
    if line[5] is None:
        line[5] = 'NA'
    if line[6] is None:
        line[6] = 'NA'
    line[8] = str(line[8])
    line[8] = line[8].replace(',','')
    line[8] = int(line[8])

    if 'Add a Plot' in line[12]:
        line[12] = 'NA'

    yield tuple(line)

def cleanDataTV(line):
    line = list(line)
    y19 =line[2].find('19')
    y20 = line[2].find('20')
    if y19 > -1:
        if y20 > -1:
            line[2] = line[2][y19:(y19+4)] + '-' + line[2][y20:(y20+4)]
        else:
            line[2] = line[2][y19:(y19+4)] + '-0000'
    elif y20 > -1:
        y20end = line[2].find('20',(y20+4))
        if y20end > -1:
            line[2] = line[2][y20:(y20+4)] + '-' + line[2][y20end:(y20end+4)]
        else:
            line[2] = line[2][y20:(y20+4)] + '-0000'
    
    indexofmin = line[4].find('min')
    if indexofmin > -1:        
        line[4] = line[4].replace(',','')
        line[4] = line[4].replace(' min','')

    elif line[4] is None:
        line[4] = '0'
    else:
        line[4] ='0'

    if line[5] is None:
        line[5] = 'NA'
    if line[6] is None:
        line[6] = 'NA'
    line[8] = str(line[8])
    line[8] = line[8].replace(',','')
    line[8] = int(line[8])

    if 'Add a Plot' in line[12]:
        line[12] = 'NA'
    yield tuple(line)
keyspace = 'technoaces'
def main():
    IMDBSparkdf = spark.read.csv('batch-8-c-100.csv', schema=imdb_schema,header=True) #.cache()
    #.cache() can be used for the above dataframe, but the system was running out of memory
    # you can try using .cache() if hardware permits
 
    IMDBMoviesRdd = IMDBSparkdf.rdd.filter(filterMovies).flatMap(cleanData).filter(lambda x: '19)' not in x[2])
    MovieDataFrame = spark.createDataFrame(IMDBMoviesRdd, imdb_schema).dropDuplicates(['title'])
    MovieDataFrame = MovieDataFrame.withColumn('runtimemins',MovieDataFrame['runtime'].cast('integer')).drop('runtime')
    MovieDataFrame.write.format("org.apache.spark.sql.cassandra").options(table='imdb_test', keyspace=keyspace).save()
    print('Movie saved')


if __name__ == '__main__':
    main()