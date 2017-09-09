# Perform batch process to generate a classfication model
# Extract TF-IDF features using spark and then train naive bayes classifier to do classification

import logging
import ConfigParser
import pandas as pd

from pyspark import SparkContext, SparkConf, SparkSession
from pyspark.ml.feature import HashingTF, IDF, Tokenizer

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
		conf=SparkConf()
		conf.setAppName('model_generation').setMaster(master)
		sc=SparkContext(conf=conf)
		spark=SparkSession.builder.config(conf).getOrCreate()
		logger.debug("Created Spark cluster successfully")
	except:
		logger.error("Fail to initialize spark cluster")

	# Input the dataset
	try:
		logger.debug("Start to read the input dataset")
		posts_df=spark.read.csv(posts_file, header=True)
		tags_df=spark.read.csv(tags_file, header=True)
		selected_tags=pd.read_csv(selected_tags_file, header=None)
		tag_set=sc.broadcast(set(selected_tags[0]))
		logger.debug("Read in dataset successfully")
	except:
		logger.debug("Can't input dataset")

	# Join posts_df and tags_df together and prepare training dataset
	selected_tags_df=tags_df.filter(tags_df['Tag'] in tag_set)
	tags_questions_df=posts_df.join(selected_tags_df, posts_df.Id==selected_tags_df.Id)
	training_df=tags_questions_df.select(['Tag', 'Body'])

	# tokenize post texts and get term frequency and inverted document frequency
	tokenizer=Tokenizer(inputCol="Body", outputCol="Words")
	tokenized_words=tokenizer.transform(training_df)
	hashing_TF=HashingTF(inputCol="Words", outputCol="Features")
	TFfeatures=hashing_TF.transform(tokenized_words)

	idf=IDF(inputCol="Features", outputCol="IDF_features")
	idfModel=idf.fit(TFfeatures)
	features=idfModel.transform(TFfeatures)

	




	




