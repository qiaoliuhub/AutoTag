import logging
import ConfigParser
import atexit

from kafka import KafkaProducer
from kafka.errors import KafkaError
import time

# Set up config, scheduler and logger
logging.basicConfig()
logger = logging.getLogger('kafka_producer')
logger.setLevel(logging.DEBUG)

config = ConfigParser.ConfigParser()
config.read('kafka_producer.cfg')

broker_ip = config.get('kafka', 'broker_ip')
posts_file = config.get('io', 'posts_file')
kafka_topic = config.get('kafka', 'topic')

def send_to_broker(kafka_producer, post):
	try:
		logger.debug('Sending data to broker')
		kafka_producer.send(topic=kafka_topic, value=post)
		logger.debug('successfully sent data %s' % post)
	except:
		logger.debug('Fail to send data %s' %post)

def shutdown_hook(kafka_producer, posts):
	try:
		logger.debug('Try to close input file')
		posts.close()
		logger.debug('Close input file successfully')
	except:
		logger.warn('Fail to close file')

	try:
		logger.debug('Try to shut down kafka producer')
		kafka_producer.flush(10)
		kafka_producer.close()
		logger.debug('Shut down kafka producer successfully')
	except:
		logger.wann('Fail to shut down kafka producer')

if __name__ == '__main__':
	
	# Start a kafka producer
	try:
		kafka_producer = KafkaProducer(bootstrap_servers=broker_ip)
		logger.debug('Create kafka producer successfully')
	except KafkaError as ke:
		logger.warn('Fail to create kafka producer, caused by %s' % ke.message)

	# Read in sample data
	try:
		logger.debug('Start to read in data')
		posts = open(posts_file, 'r')
		logger.debug('Read in data successfully')
	except:
		logger.debug('Fail to read data')

	atexit.register(shutdown_hook, posts, kafka_producer)

	# Sending data to broker
	while True:
		for line in posts:
			send_to_broker(kafka_producer, line,)
			time.sleep(0.001)

