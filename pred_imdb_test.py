from pyspark.sql import SparkSession, functions, types
import sys
from pyspark.ml import PipelineModel
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

    # get the data
    test_votes= spark.read.format("org.apache.spark.sql.cassandra")\
                .options(table='imdb_test', keyspace=keyspace).load()
    test_votes = test_votes.where(test_votes['imdb_score']!=0).where(test_votes['runtimemins']!=0)\
                            .where(test_votes['meta_score']!=0).where(test_votes['votes']!=0)
    
    model = PipelineModel.load(model_file)

    predictions = model.transform(test_votes)
    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='imdb_score',
            metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)
    
    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='imdb_score',
            metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)

    print('r2 =', r2)
    print('rmse =', rmse)

    return 0

if __name__ == '__main__':
    model_file = sys.argv[1]
    main(model_file)