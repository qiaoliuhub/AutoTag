import logging
import ConfigParser
import redis
import json

from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Set up logger
logging.basicConfig()
logger = logging.getLogger('redis_consumer')
logger.setLevel(logging.DEBUG)

# Read in config file
config = ConfigParser.ConfigParser()
config.read('redis_consumer.cfg')

# Read in parameter from config file
kafka_broker = config.get('kafka', 'broker')
kafka_topic = config.get('kafka', 'topic')
kafka_consumer = None
redis_host = config.get('redis', 'host')
redis_port = config.get('redis', 'port')
redis_client = None

def process_data(kafka_msg):

	data = json.loads(kafka_msg)
	logger.debug("Load in data %s" %data['Id'])
	try:
		logger.debug("Writing data to redis_client")
		redis_client.hmset(data['Id'], data)
		logger.debug("Write data to redis_client successfully")
	except:
		logger.debug('Fail to write data to redis')

if __name__ == '__main__':
	
	# Creat a kafka consumer
	try:
		logger.debug("Initiating kafka consumer")
		kafka_consumer = KafkaConsumer(kafka_topic ,bootstrap_servers=[kafka_broker])
		logger.debug("Initiated kafka consumer successfully")
	except KafkaError as ke:
		logger.debug("Fail to start kafka consumer, caused by %s" %ke.message)

	# Connect to redis
	try:
		logger.debug("Connecting to redis")
		redis_client = redis.StrictRedis(host=redis_host, port=redis_port)
		logger.debug("Initiated redis client successfully")
	except Exception as e:
		logger.debug("Fail to connect to redis, caused by %s" % e.message)

	logger.debug("Start to write data to redis")
	for msg in kafka_consumer:
		process_data(msg.value)



