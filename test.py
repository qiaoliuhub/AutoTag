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
from pyspark.sql.types import StringType, FloatTypex

#Set up logger
logging.BasicConfig()
logger = logging.getLogger('Streaming_prediction')
logger.setLevel(logging.DEBUG)

# set up configuration file parser
config = ConfigParser.ConfigParser()
config.read('streaming_prediction')

master = config.get('spark', 'master')
broker_ip = config.get('kafka', 'broker_ip')
kafka_topic = config.get('kafka', 'kafka_topic')
kafka_output_topic = config.get('kafka', 'kafka_output_topic')

tokenizer_file = config.get('io', 'tokenizer_file')
hashing_tf_file = config.get('io', 'hashing_tf_file')
idf_model_file = config.get('io', 'idf_model_file')
nb_model_file = config.get('io', 'nb_model_file')

idf_model = None
nb_model = None
hashing_tf = None
tokenizer = None

tags_to_catId_transform = None
catId_to_tags_transform = None


def process_data(dStream, kafka_producer):

	def features_extraction(df):
		# Extract featrues
		try:
			logger.debug('Extracting features from data')
			words_df = tokenizer.transform(df)
			tf_features_df = hashing_tf.transform(words_df)
			tf_idf_features_df = idf_model.transform(tf_features_df)
			logger.debug('Extract features successfully')
			return tf_features_df
		except:
			logger.warn('Fail to extract features from Questions')

	def predict_tag(df):
		# Predict the tags according to extracted features
		try:
			logger.debug('Predicting data tag')
			post_data = df.withColumn('CatId', tags_to_catId_transform('Tag'))
			prediction = nb_model.transform(post_data)
			output_data = prediction.withColumn('Predicted_tag', catId_to_tags('CatId'))
			logger.debug('Predicted tags are generated')
			return output_data
		except:
			logger.warn('Fail to predict tags')

	# Write data back to Kafka producer
	def persist_data(row):
		tagged_data = json.dumps(row.asDict())
		try:
			logger.debug('Wrting data to Kafka topic %s' % kafka_output_topic)
			kafka_producer.send(kafka_output_topic, value=tagged_data)
			logger.info('sent data successfully')
		except:
			logger.debug('Fail to send stock data %s' % tagged_data)

	stream_df = spark.read.json(dStream)
	features_df = features_extraction(stream_df)
	predictions = predict_tag(features_df)
	predictions.foreach(persist_data)


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

	try;
		logger.debug('Shut down spark context')
		spark.close()
		logger.debug('Stop spark successfully')
	except:
		logger.warn('Fail to stop spark')


if __name__ == '__main__':
	
	#build spark context
	try:
		logger.debug('Set up sparkcontext and sparkstreamingcontext')
		conf = SparkConf().setAppName('Streaming_prediction').setMaster(master)
		sc = SparkContext(conf=conf)
		sc.setLogLevel('INFO')
		ssc = StreamingContext(sc, 5)
		logger.debug('Initialize spark context and sparkstreamingcontext successfully')
	except Exception as e:
		logger.debug('Fail to start spark context and sparkstreamingcontext')
		raise
	finally:
		sc.close()

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
		directKafkaStream = KafkaUtils.createDirectStream(ssc, kafka_topic, {'metadata.broker.list' = broker_ip})
		logger.debug('Create direct dstream from kafka successfully')
	except:
		logger.debug('Unable to create dstream from kafka')

	atexit.register(shutdown_hook, kafka_producer, spark)

	# Load in idf_model, nb_model, hashing_tf, idf_model and tag_catId map
	try:
		logger.debug('Loading models')
		tokenizer = Tokenizer.load(tokenizer_file)
		hashing_tf = HashingTF.load(hashing_tf_file)
		idf_model = IDFModel.load(idf_model_file)
		nb_model = NaiveBayesModel.load(nb_model_file)
		selected_tags = pd.read_csv(selected_tags_file, header=None)
		local_catId_to_tags = dict(zip(list(selected_tags.index), selected_tags[0]))
		local_tags_to_catId=dict(zip(selected_tags[0], list(selected_tags.index)))
		catId_to_tags = sc.broadcast(local_catId_to_tags)
		tags_to_catId = sc.broadcast(local_tags_to_catId)
		tags_to_catId_transform = udf(lambda tag: float(tags_to_catId.value[tag]), FloatType())
		catId_to_tags_transform = udf(lambda catId: catId_to_tags.value[catId], StringType())
		logger.debug('loaded models successfully')
	except:
		logger.debug('Fail to load models')


	logger.debug('Start to process data')
	process_data(directKafkaStream, kafka_producer)
	ssc.start()
	ssc.awaitTermination()




















