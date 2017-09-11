# Perform batch process to generate a classfication model
# Extract TF-IDF features using spark and then train naive bayes classifier to do classification

import logging
import ConfigParser
import pandas as pd
import csv
import atexit

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, IDFModel
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import udf, col


logging.basicConfig()
logger=logging.getLogger('model_generation')
logger.setLevel(logging.DEBUG)

config=ConfigParser.ConfigParser()
config.read('model_generation.cfg')

master=config.get('spark','master')
posts_file=config.get('io', 'post_file')
tags_file=config.get('io', 'tags_file')
selected_tags_file=config.get('io', 'selected_tags_file')

idf_model_file=config.get('io','idf_model_file')
nb_model_file=config.get('io','nb_model_file')

def shutdown_hook(spark_session):
	try:
		spark_session.close()
		logger.debug("Successfully stop spark session and spark context")
	except:
		logger.debug("Fail to stop spark session and spark context")

if __name__ == '__main__':

	# Try to initialize a spark cluster with master, master can be local or mesos URL, which is configurable in config file 
	try:
		logger.debug("Initializing Spark cluster")
		conf=SparkConf().setAppName('model_generation').setMaster(master)
		sc=SparkContext(conf=conf)
		logger.debug("Created Spark cluster successfully")
	except:
		logger.error("Fail to initialize spark cluster")

	try:
		spark=SparkSession.builder.config(conf=conf).getOrCreate()
		logger.debug("Initialized spark session successfully")
	except:
		logger.error("Fail to start spark session")

	# Input the dataset
	try:
		logger.debug("Start to read the input dataset")
		posts_df=spark.read.json(posts_file)
		tags_df=spark.read.csv(tags_file, header=True)
		selected_tags=pd.read_csv(selected_tags_file, header=None)
		local_tags_to_catId=dict(zip(selected_tags[0], list(selected_tags.index)))
		local_catId_to_tags=dict(zip(list(selected_tags.index), selected_tags[0]))
		tags_to_catId=sc.broadcast(local_tags_to_catId)
		catId_to_tags=sc.broadcast(local_catId_to_tags)
		tags_set=sc.broadcast(set(selected_tags[0]))
		logger.debug("Read in dataset successfully")
		
	except:
		logger.error("Can't input dataset")

	# Join posts_df and tags_df together and prepare training dataset
	selected_tags_df=tags_df.filter(tags_df.Tag.isin(tags_set.value)).na.drop(how = 'any')
	tags_questions_df=selected_tags_df.join(posts_df, "Id")
	training_df=tags_questions_df.select(['Tag', 'Body','Id']).na.drop(how = 'any')
	logger.debug("successfully get training_df")

	# tokenize post texts and get term frequency and inverted document frequency
	logger.debug("Start to generate TFIDF features")
	tokenizer=Tokenizer(inputCol="Body", outputCol="Words")
	tokenized_words=tokenizer.transform(training_df.na.drop(how = 'any'))
	hashing_TF=HashingTF(inputCol="Words", outputCol="Features")#, numFeatures=200
	TFfeatures=hashing_TF.transform(tokenized_words.na.drop(how = 'any'))

	idf=IDF(inputCol="Features", outputCol="IDF_features")
	idfModel=idf.fit(TFfeatures.na.drop())
	idfModel.save(idf_model_file)
	TFIDFfeatures=idfModel.transform(TFfeatures.na.drop(how = 'any'))
	logger.debug("Get TFIDF features successfully")

	# for feature in TFIDFfeatures.select("IDF_features", "Tag").take(3):
	# 	logger.info(feature) 

	# register shutdown_hook
	atexit.register(shutdown_hook, spark_session=spark)

	# Row(IDF_features=SparseVector(200, {7: 2.3773, 9: 2.1588, 32: 2.0067, 37: 1.7143, 49: 2.6727, 59: 2.9361, 114: 1.0654, 145: 2.9522, 167: 2.3751}), Tag=u'asp.net')
	# Trasfer data to be in labeled point format

	labeled_points=TFIDFfeatures.rdd.map(lambda row: (float(tags_to_catId.value[row.Tag]), row.IDF_features, row.Id)).toDF()
	training, test=labeled_points.randomSplit([0.7, 0.3], seed=0)

	# Train Naive Bayes model
	nb=NaiveBayes(smoothing=1.0, modelType="multinomial", labelCol='_1', featuresCol='_2')
	nb_model=nb.fit(training)
	nb_model.save(nb_model_file)

	# Evaluation the model
	# test_df=test.rdd.map(lambda row: ((row._2, row._3),[row._1])).reduceByKey(lambda a,b: a+b)
	# print test_df.collect()

	predictions=nb_model.transform(test)
	evaluator=MulticlassClassificationEvaluator(labelCol="_1", predictionCol="prediction", metricName="accuracy")
	accuracy = evaluator.evaluate(predictions)
	print("Test set accuracy = " + str(accuracy))

	# prediction_and_label = test.map(lambda point : (nb_model.predict(point.features), point.label))
	# accuracy = 1.0 * prediction_and_label.filter(lambda x: 1.0 if x[0] == x[1] else 0.0).count() / test.count()

	









	




