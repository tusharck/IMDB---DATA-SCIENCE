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

spark = SparkSession.builder.appName('imdb etl').getOrCreate()
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
def main(inputs):
    os.chdir('..')
    IMDBSparkdf = spark.read.csv(str(os.getcwd()) +'/' + inputs, schema=imdb_schema,header=True) #.cache()
    #.cache() can be used for the above dataframe, but the system was running out of memory
    # you can try using .cache() if hardware permits
    
    IMDBMoviesRdd = IMDBSparkdf.rdd.filter(filterMovies).flatMap(cleanData).filter(lambda x: '19)' not in x[2])
    MovieDataFrame = spark.createDataFrame(IMDBMoviesRdd, imdb_schema).dropDuplicates(['title'])
    MovieDataFrame = MovieDataFrame.withColumn('runtimemins',MovieDataFrame['runtime'].cast('integer')).drop('runtime')
    MovieDataFrame.write.csv((str(os.getcwd()) + '/data_cleaned/movies'), mode='overwrite')
    print('Movie saved')

    IMDBTVRdd = IMDBSparkdf.rdd.filter(filter_tv).flatMap(cleanDataTV)
    TVDataFrame = spark.createDataFrame(IMDBTVRdd, imdb_schema).dropDuplicates(['title'])
    TVDataFrame = TVDataFrame.withColumn('runtimemins',TVDataFrame['runtime'].cast('integer'))\
                            .withColumn('start_year', split(TVDataFrame['year'], '-').getItem(0).cast('integer')) \
                            .withColumn('end_year', split(TVDataFrame['year'], '-').getItem(1).cast('integer'))\
                            .drop('year').drop('runtime').drop('meta_score')
    
    TVDataFrame.write.csv((str(os.getcwd()) + '/data_cleaned/tvshow'), mode='overwrite')
    print('TV Show saved')

    IMDBVideoGameRdd = IMDBSparkdf.rdd.filter(filter_games).flatMap(cleanData)
    VideoGameDataFrame = spark.createDataFrame(IMDBVideoGameRdd, imdb_schema)
    VideoGameDataFrame = VideoGameDataFrame.dropDuplicates(['title']).drop('runtime').drop('meta_score')
    VideoGameDataFrame.write.csv((str(os.getcwd()) + '/data_cleaned/videogame'), mode='overwrite')
    print('Video Game saved')
    

    IMDBTVMovieRdd = IMDBSparkdf.rdd.filter(filter_tvmovie).flatMap(cleanData).filter(lambda x: 'TV' not in x[2])
    TVMovieDataFrame = spark.createDataFrame(IMDBTVMovieRdd, imdb_schema).dropDuplicates(['title'])
    TVMovieDataFrame = TVMovieDataFrame.withColumn('runtimemins',TVMovieDataFrame['runtime'].cast('integer'))\
                                .drop('runtime').drop('meta_score')
    TVMovieDataFrame.write.csv((str(os.getcwd()) + '/data_cleaned/tvmovie'), mode='overwrite')
    print('TV Movie saved')

    IMDBVideoRdd = IMDBSparkdf.rdd.filter(filter_video).flatMap(cleanData).filter(lambda x: 'ideo' not in x[2])
    VideoDataFrame = spark.createDataFrame(IMDBVideoRdd, imdb_schema).dropDuplicates(['title'])
    VideoDataFrame = VideoDataFrame.withColumn('runtimemins',VideoDataFrame['runtime'].cast('integer'))\
                                .drop('runtime').drop('meta_score')
    VideoDataFrame.write.csv((str(os.getcwd()) + '/data_cleaned/video'), mode='overwrite')
    print('Video saved')

    IMDBTVSpecialRdd = IMDBSparkdf.rdd.filter(filter_tvspecial).flatMap(cleanData).filter(lambda x: 'TV' not in x[2])
    TVSpecialDataFrame = spark.createDataFrame(IMDBTVSpecialRdd, imdb_schema).dropDuplicates(['title'])
    TVSpecialDataFrame = TVSpecialDataFrame.withColumn('runtimemins',TVSpecialDataFrame['runtime'].cast('integer'))\
                                            .drop('runtime').drop('meta_score')
    TVSpecialDataFrame.write.csv((str(os.getcwd()) + '/data_cleaned/tvspecial'), mode='overwrite')
    print('TV Special saved')
    
    IMDBTVShortRdd = IMDBSparkdf.rdd.filter(filter_tvshort).flatMap(cleanData).filter(lambda x: 'TV' not in x[2])
    TVShortDataFrame = spark.createDataFrame(IMDBTVShortRdd, imdb_schema).dropDuplicates(['title'])
    TVShortDataFrame = TVShortDataFrame.withColumn('runtimemins',TVShortDataFrame['runtime'].cast('integer'))\
                                        .drop('runtime').drop('meta_score')
    
    TVShortDataFrame.write.csv((str(os.getcwd()) + '/data_cleaned/tvshort'), mode='overwrite')
    print('TV Short saved')

if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)