from pyspark.sql import SparkSession, types

imdb_schema = types.StructType([
    types.StructField('ID', types.LongType(), True),
    types.StructField('Movie', types.StringType(), True),
    types.StructField('Year', types.StringType(), True),
    types.StructField('Genre', types.StringType(), True),
    types.StructField('Runtime', types.StringType(), True),
    types.StructField('IMDBScore', types.FloatType(), True),
    types.StructField('MetaScore', types.FloatType(), True),
    types.StructField('Votes', types.StringType(), True),
    types.StructField('Director', types.StringType(), True),
    types.StructField('stars', types.StringType(), True),
    types.StructField('Discription', types.StringType(), True),
    types.StructField('Image', types.StringType(), True)
    ])