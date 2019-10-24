from pyspark.sql import SparkSession, types

movie_schema = types.StructType([
    types.StructField('imdb_id', types.StringType(), True),
    types.StructField('title', types.StringType(), True),
    types.StructField('year', types.StringType(), True),
    types.StructField('genre', types.StringType(), True),
    types.StructField('country', types.StringType(), True),
    types.StructField('language', types.StringType(), True),
    types.StructField('imdb_score', types.FloatType(), True),
    types.StructField('meta_score', types.IntegerType(), True),
    types.StructField('votes', types.IntegerType(), True),
    types.StructField('director', types.StringType(), True),
    types.StructField('stars', types.StringType(), True),
    types.StructField('description', types.StringType(), True),
    types.StructField('image', types.StringType(), True),
    types.StructField('runtimemins', types.IntegerType(), True)
    ])

tvshow_schema = types.StructType([
    types.StructField('imdb_id', types.StringType(), True),
    types.StructField('title', types.StringType(), True),
    types.StructField('genre', types.StringType(), True),
    types.StructField('country', types.StringType(), True),
    types.StructField('language', types.StringType(), True),
    types.StructField('imdb_score', types.FloatType(), True),
    types.StructField('meta_score', types.IntegerType(), True),
    types.StructField('votes', types.IntegerType(), True),
    types.StructField('director', types.StringType(), True),
    types.StructField('stars', types.StringType(), True),
    types.StructField('description', types.StringType(), True),
    types.StructField('image', types.StringType(), True),
    types.StructField('runtimemins', types.IntegerType(), True),
    types.StructField('start_year', types.IntegerType(), True),
    types.StructField('end_year', types.IntegerType(), True)
    ])

video_game_schema = types.StructType([
    types.StructField('imdb_id', types.StringType(), True),
    types.StructField('title', types.StringType(), True),
    types.StructField('year', types.StringType(), True),
    types.StructField('genre', types.StringType(), True),
    types.StructField('country', types.StringType(), True),
    types.StructField('language', types.StringType(), True),
    types.StructField('imdb_score', types.FloatType(), True),
    types.StructField('votes', types.IntegerType(), True),
    types.StructField('director', types.StringType(), True),
    types.StructField('stars', types.StringType(), True),
    types.StructField('description', types.StringType(), True),
    types.StructField('image', types.StringType(), True)
    ])

imdb_general_schema = types.StructType([
    types.StructField('imdb_id', types.StringType(), True),
    types.StructField('title', types.StringType(), True),
    types.StructField('year', types.StringType(), True),
    types.StructField('genre', types.StringType(), True),
    types.StructField('country', types.StringType(), True),
    types.StructField('language', types.StringType(), True),
    types.StructField('imdb_score', types.FloatType(), True),
    types.StructField('votes', types.IntegerType(), True),
    types.StructField('director', types.StringType(), True),
    types.StructField('stars', types.StringType(), True),
    types.StructField('description', types.StringType(), True),
    types.StructField('image', types.StringType(), True),
    types.StructField('runtimemins', types.IntegerType(), True)
    ])