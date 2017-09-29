import logging
import ConfigParser
import atexit
import json
import pandas as pd

from pyspark import SparkContext, SparkConf 
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from kafka import KafkaProducer
from pyspark.streaming.kafka import KafkaUtils
from kafka.errors import KafkaError
from pyspark.ml.feature import Tokenizer, HashingTF, IDFModel
from pyspark.ml.classification import NaiveBayesModel
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType
from apscheduler.schedulers.background import BackgroundScheduler

#Set up logger
logging.basicConfig()
logger = logging.getLogger('Streaming_prediction')
logger.setLevel(logging.DEBUG)

# set up configuration file parser
config = ConfigParser.ConfigParser()
config.read('streaming_prediction.cfg')

# Set up scheduler
schedule = BackgroundScheduler()
schedule.add_executor('threadpool')
schedule.start()

master = config.get('spark', 'master')
broker_ip = config.get('kafka', 'broker_ip')
kafka_topic = config.get('kafka', 'kafka_topic')
kafka_output_topic = config.get('kafka', 'kafka_output_topic')

tokenizer_file = config.get('io', 'tokenizer_file')
hashing_tf_file = config.get('io', 'hashing_tf_file')
idf_model_file = config.get('io', 'idf_model_file')
nb_model_file = config.get('io', 'nb_model_file')
selected_tags_file = config.get('io', 'selected_tags_file')

idf_model = None
nb_model = None
hashing_tf = None
tokenizer = None
catId_to_tags_transform = None


def process_data(rdd, kafka_producer):

	def features_extraction(df):
		# Extract featrues
		try:
			logger.debug('Extracting features from data')
			words_df = tokenizer.transform(df)
			tf_features_df = hashing_tf.transform(words_df)
			tf_idf_features_df = idf_model.transform(tf_features_df)
			logger.debug('Extract features successfully' )
			return tf_idf_features_df
		except:
			logger.warn('Fail to extract features from Questions')

	def predict_tag(df):
		# Predict the tags according to extracted features
		try:
			logger.debug('Predicting data tag')
			prediction = nb_model.transform(df)
			logger.debug('Predicted tags are generated')
			logger.debug('Transform catId to tags')
			output_data = prediction.withColumn('tags', catId_to_tags_transform('prediction')).drop('Features', 'IDF_features','prediction','rawPrediction','Words', 'probability')
			logger.debug('Transform catId to tas successfully')
			return output_data
		except:
			logger.warn('Fail to predict tags')

	# Write data back to Kafka producer
	def persist_data(df):
		posts = df.toJSON().collect()
		for r in posts:
			try:
				logger.debug('Wrting data to Kafka topic %s' % kafka_output_topic)
				kafka_producer.send(kafka_output_topic, value=r.encode('utf-8'))
				logger.info('sent data successfully')
			except KafkaError as ke:
				logger.debug('Fail to send stock data, caused by %s' %ke.message)

	if rdd.isEmpty():
		return
	stream_df = spark.read.json(rdd)
	features_df = features_extraction(stream_df)
	predictions = predict_tag(features_df)
	persist_data(predictions)

def update_models():
	# Load in idf_model, nb_model, hashing_tf, idf_model and tag_catId map
	logger.debug('===================================================Starting load models===================================================')
	try:
		logger.debug('Loading tokenizer model')
		new_tokenizer = Tokenizer.load(tokenizer_file)
		logger.debug('Load tokenizer model successfully')
	except:
		logger.debug('Fail to load tokenizer')

	try:
		logger.debug('Loading hashing_tf model')
		new_hashing_tf = HashingTF.load(hashing_tf_file)
		logger.debug('Load hashing_tf model successfully')
	except:
		logger.debug('Fail to load hashing_tf')

	try:
		logger.debug('Loading idf_model')
		new_idf_model = IDFModel.load(idf_model_file)
		logger.debug('Load IDFModel successfully')
	except:
		logger.debug('Fail to load IDFModel')

	try:
		logger.debug('Loading nb_model')
		new_nb_model = NaiveBayesModel.load(nb_model_file)
		logger.debug('Load NaiveBayesModel successfully')
	except:
		logger.debug('Fail to load NaiveBayesModel')

	try:
		logger.debug('Updating models')
		tokenizer = new_tokenizer
		hashing_tf = new_hashing_tf
		idf_model = new_idf_model
		nb_model = new_nb_model
		logger.debug('update model successfully')
	except:
		logger.debug('Fail to update models')
	logger.debug('===================================================Stopped load models===================================================')



# Create shut down hook
def shutdown_hook(kafka_producer, spark):
	# Shut down kafka producer
	try:
		logger.debug('Closing kafka producer')
		kafka_producer.flush(10)
		kafka_producer.close()
		logger.debug('Stop kafka producer successfully')
	except KafkaError as ke:
		logger.warn('Fail to stop kafka producer, caused by %s' % ke.message)

	try:
		logger.debug('Shut down spark context')
		spark.close()
		logger.debug('Stop spark successfully')
	except:
		logger.warn('Fail to stop spark')

	try:
		logger.debug('Shut down scheduler')
		schedule.shutdown();
		logger.debug('Shut down scheduler successfully')
	except:
		logger.warn('Fail to shut down scheduler')


if __name__ == '__main__':
	
	#build spark context
	try:
		logger.debug('Set up sparkcontext and sparkstreamingcontext')
		conf = SparkConf().setAppName('Streaming_prediction').setMaster(master)
		sc = SparkContext(conf=conf)
		sc.setLogLevel('INFO')
		ssc = StreamingContext(sc, 10)
		logger.debug('Initialize spark context and sparkstreamingcontext successfully')
	except Exception as e:
		logger.debug('Fail to start spark context and sparkstreamingcontext')

	# Start a sparksession
	try:
		logger.debug('Set up SparkSession')
		spark = SparkSession.builder.getOrCreate()
		logger.debug('Start spark session successfully')
	except:
		logger.debug('Fail to start sparksession')

	# Connect to Kafka broker
	try:
		# Create kafka producer
		logger.debug('Initialize kafka producer')
		kafka_producer = KafkaProducer(bootstrap_servers=broker_ip)
		logger.debug('Start kafka producer successfully')
	except KafkaError as ke:
		logger.debug('Fail to start kafka producer, caused by %s' % ke.message)

	try:
		# Create dstream from kafka topic
		directKafkaStream = KafkaUtils.createDirectStream(ssc, [kafka_topic], {'metadata.broker.list':broker_ip})
		logger.debug('Create direct dstream from kafka successfully')
	except Exception as e:
		logger.debug('Unable to create dstream from kafka, caused by %s', e.message)

	atexit.register(shutdown_hook, kafka_producer, spark)

	# Load in idf_model, nb_model, hashing_tf, idf_model and tag_catId map
	try:
		logger.debug('Loading tokenizer model')
		tokenizer = Tokenizer.load(tokenizer_file)
		logger.debug('Load tokenizer model successfully')
	except:
		logger.debug('Fail to load tokenizer')

	try:
		logger.debug('Loading hashing_tf model')
		hashing_tf = HashingTF.load(hashing_tf_file)
		logger.debug('Load hashing_tf model successfully')
	except:
		logger.debug('Fail to load hashing_tf')

	try:
		logger.debug('Loading idf_model')
		idf_model = IDFModel.load(idf_model_file)
		logger.debug('Load IDFModel successfully')
	except:
		logger.debug('Fail to load IDFModel')

	try:
		logger.debug('Loading nb_model')
		nb_model = NaiveBayesModel.load(nb_model_file)
		logger.debug('Load NaiveBayesModel successfully')
	except:
		logger.debug('Fail to load NaiveBayesModel')

	try:
		logger.debug('Loading files')
		selected_tags = pd.read_csv(selected_tags_file, header=None)
		logger.debug('loaded files successfully ')
	except:
		logger.debug('Fail to load files')

	local_catId_to_tags = dict(zip(list(selected_tags.index), selected_tags[0]))
	catId_to_tags = sc.broadcast(local_catId_to_tags)
	catId_to_tags_transform = udf(lambda catId: catId_to_tags.value[int(catId)], StringType())

	logger.debug('Start to process data')
	directKafkaStream.map(lambda dStream: dStream[1]).foreachRDD(lambda rdd: process_data(rdd, kafka_producer))
	logger.debug('After function')

	schedule.add_job(update_models, 'interval', hours = 1)

	ssc.start()
	ssc.awaitTermination()

	


