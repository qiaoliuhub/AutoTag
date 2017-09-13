import logging
import ConfigParser
import pandas as pd
import atexit

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, IDFModel
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import NaiveBayes, NaiveBayesModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import udf, col
from kafka import KafkaProducer
from pyspark.streaming.kafka import KafkaUtils
from kafka.errors import KafkaError


logging.basicConfig()
logger=logging.getLogger('model_generation')
logger.setLevel(logging.DEBUG)

config=ConfigParser.ConfigParser()
config.read('model_generation.cfg')

master=config.get('spark','master')

idf_model_file=config.get('io','idf_model_file')
nb_model_file=config.get('io','nb_model_file')
hashing_tf_file=config.get('io', 'hashing_tf_file')
tokenizer_file=config.get('io', 'tokenizer_file')

def process_dStream(dStream):


if __name__ == '__main__':

	# Try to initialize a spark cluster with master, master can be local or mesos URL, which is configurable in config file 
	try:
		logger.debug("Initializing Spark cluster")
		conf=SparkConf().setAppName('model_generation').setMaster(master)
		sc=SparkContext(conf=conf)
		sc.setLogLevel('INFO')
		ssc=StreamingContext(sc, 5)
		logger.debug("Created Spark cluster successfully")
	except:
		logger.error("Fail to initialize spark cluster")

	try:
		spark=SparkSession.builder.config(conf=conf).getOrCreate()
		logger.debug("Initialized spark session successfully")
	except:
		logger.error("Fail to start spark session")

	try:
		# Cread Dstream from multiple kafka topics and create a microbatch every 5 seconds
		directKafkaStream=KafkaUtils.createDirectStream(ssc, READ_TOPICS, {'metadata.broker.list':BROKER})
		logger.info('Create spark direct stream successfully')

	except:
		logger.debug('Fail to create direct stream')

	logger.info('Start to process data')
	process_dStream(directKafkaStream)

	try:
		# Create Kafka producer
		KAFKA_PRODUCER=KafkaProducer(bootstrap_servers=BROKER)
		logger.info('Create kafka producer successfully')

	except KafkaError as ke:
		logger.debug('Fail to create kafka producer, caused by %s' % ke.message)













