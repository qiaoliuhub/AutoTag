# Perform batch process to generate a classfication model
# Extract TF-IDF features using spark and then train naive bayes classifier to do classification

import logging
import ConfigParser
import pandas as pd

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import SparseVector
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


logging.basicConfig()
logger=logging.getLogger('model_generation')
logger.setLevel(logging.DEBUG)

config=ConfigParser.ConfigParser()
config.read('model_generation.cfg')

master=config.get('spark','master')
posts_file=config.get('io', 'post_file')
tags_file=config.get('io', 'tags_file')
selected_tags_file=config.get('io', 'selected_tags_file')


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
		posts_df=spark.read.csv(posts_file, header=True)
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
	selected_tags_df=tags_df.filter(tags_df.Tag.isin(tags_set.value))
	tags_questions_df=posts_df.join(selected_tags_df, posts_df.Id==selected_tags_df.Id)
	training_df=tags_questions_df.select(['Tag', 'Body'])

	# tokenize post texts and get term frequency and inverted document frequency
	tokenizer=Tokenizer(inputCol="Body", outputCol="Words")
	tokenized_words=tokenizer.transform(training_df)
	hashing_TF=HashingTF(inputCol="Words", outputCol="Features", numFeatures=200)
	TFfeatures=hashing_TF.transform(tokenized_words)

	idf=IDF(inputCol="Features", outputCol="IDF_features")
	idfModel=idf.fit(TFfeatures)
	TFIDFfeatures=idfModel.transform(TFfeatures)

	for feature in TFIDFfeatures.select("IDF_features", "Tag").take(3):
		logger.info(feature) 

	# Row(IDF_features=SparseVector(200, {7: 2.3773, 9: 2.1588, 32: 2.0067, 37: 1.7143, 49: 2.6727, 59: 2.9361, 114: 1.0654, 145: 2.9522, 167: 2.3751}), Tag=u'asp.net')
	# Trasfer data to be in labeled point format
	labeled_points=TFIDFfeatures.rdd.map(lambda row: LabeledPoint(label=tags_to_catId.value[row.Tag], features=SparseVector(row.IDF_features.size, row.IDF_features.indices, row.IDF_features.values)))
	training, test=labeled_points.randomSplit([0.7, 0.3], seed=0)

	# Train Naive Bayes model
	print training.take(3)
	nb=NaiveBayes(smoothing=1.0, modelType="multinomial")#
	nb_model=nb.fit(training)

	 # Evaluation the model
	predictions=nb_model.transform(test)
	print predictions.take(10)
	# evaluator=MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
	# prediction_and_label = test.map(lambda point : (nb_model.predict(point.features), point.label))
	# accuracy = 1.0 * prediction_and_label.filter(lambda x: 1.0 if x[0] == x[1] else 0.0).count() / test.count()








	




