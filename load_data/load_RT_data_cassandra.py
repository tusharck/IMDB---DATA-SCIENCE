from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
from load_imdb_tools import movie_schema,tvshow_schema,video_game_schema,imdb_general_schema
import os 
import sys


cluster_seeds = ['127.0.0.1']
#cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('imdb etl').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()

keyspace = 'technoaces'
rt_schema = types.StructType([
    types.StructField('audience_average', types.FloatType(), True),
    types.StructField('audience_percent', types.FloatType(), True),
    types.StructField('audience_ratings', types.FloatType(), True),
    types.StructField('critic_average', types.FloatType(), True),
    types.StructField('critic_percent', types.FloatType(), True),
    types.StructField('imdb_id', types.StringType(), True),
    types.StructField('rotten_tomatoes_id', types.StringType(), True)
    ])

def main():
    os.chdir('..')
    RTDataFrame = spark.read.json(str(os.getcwd()) +'/data_cleaned/rotten-tomatoes', schema=rt_schema)
    RTDataFrame.write.format("org.apache.spark.sql.cassandra").options(table='rotten_tomatoes', keyspace=keyspace).save()



if __name__ == '__main__':
    #inputs = sys.argv[1]
    main()