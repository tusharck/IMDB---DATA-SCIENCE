from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
from load_imdb_tools import movie_schema,tvshow_schema,video_game_schema,imdb_general_schema
import os 
import sys


cluster_seeds = ['127.0.0.1']
#cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('imdb etl').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()

keyspace = 'technoaces'


def main():
    os.chdir('..')
    MovieDataFrame = spark.read.csv(str(os.getcwd()) +'/data_cleaned/movies', schema=movie_schema)
    MovieDataFrame.write.format("org.apache.spark.sql.cassandra").options(table='imdb_movies_data', keyspace=keyspace).save()

    TVDataFrame = spark.read.csv(str(os.getcwd()) +'/data_cleaned/tvshow', schema=tvshow_schema)
    TVDataFrame.write.format("org.apache.spark.sql.cassandra").options(table='imdb_tv_show_data', keyspace=keyspace).save()

    VideoGameDataFrame = spark.read.csv(str(os.getcwd()) +'/data_cleaned/videogame', schema=video_game_schema)
    VideoGameDataFrame.write.format("org.apache.spark.sql.cassandra").options(table='imdb_video_game_data', keyspace=keyspace).save()

    TVMovieDataFrame = spark.read.csv(str(os.getcwd()) +'/data_cleaned/tvmovie', schema=imdb_general_schema)
    TVMovieDataFrame.format("org.apache.spark.sql.cassandra").options(table='imdb_video_data', keyspace=keyspace).save()

    VideoDataFrame = spark.read.csv(str(os.getcwd()) +'/data_cleaned/video', schema=imdb_general_schema)
    VideoDataFrame.write.format("org.apache.spark.sql.cassandra").options(table='imdb_tv_movie_data', keyspace=keyspace).save()

    TVSpecialDataFrame = spark.read.csv(str(os.getcwd()) +'/data_cleaned/tvspecial', schema=imdb_general_schema)
    TVSpecialDataFrame.write.format("org.apache.spark.sql.cassandra").options(table='imdb_tv_special_data', keyspace=keyspace).save()
    
    TVShortDataFrame = spark.read.csv(str(os.getcwd()) +'/data_cleaned/tvshort', schema=imdb_general_schema)
    TVShortDataFrame.write.format("org.apache.spark.sql.cassandra").options(table='imdb_tv_short_data', keyspace=keyspace).save()



if __name__ == '__main__':
    #inputs = sys.argv[1]
    main()