from pyspark.sql import SparkSession, functions, types
import sys

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql.functions import dayofyear
from pyspark.ml.evaluation import RegressionEvaluator

cluster_seeds = ['127.0.0.1']
#cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('Analysis Part 1').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
spark.sparkContext.setLogLevel('WARN')

keyspace = 'technoaces'
def main(model_file):

    keyspace='technoaces'

    data = spark.read.format("org.apache.spark.sql.cassandra")\
                .options(table='imdb_movies_data', keyspace=keyspace).load()
    data = data.where(data['imdb_score']!=0).where(data['runtimemins']!=0).where(data['meta_score']!=0).where(data['votes']!=0)
    train, validation = data.randomSplit([0.75, 0.25])

    imdb_assembler = VectorAssembler(
        inputCols=['year','runtimemins','meta_score', 'votes'],
        outputCol='features')
    
    imdbclassifier = RandomForestRegressor(
        numTrees=2,featuresCol='features',
        labelCol='imdb_score',maxDepth=30,seed=1000)

    pipeline = Pipeline(stages=[imdb_assembler, imdbclassifier])
    
    model = pipeline.fit(train)
    predictions = model.transform(validation)

    predictions.select('imdb_id','title','runtimemins','meta_score', 'imdb_score','votes'\
                ,predictions['prediction'].alias('Predicted Votes')).show()
    
    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='imdb_score',
            metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)

    return 0

if __name__ == '__main__':
    model_file = sys.argv[1]
    main(model_file)