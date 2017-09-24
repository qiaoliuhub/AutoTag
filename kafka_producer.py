import logging
import ConfigParser
import atexit
import time

from apscheduler.schedulers.background import BackgroundScheduler
from kafka import KafkaProducer
from kafka.errors import KafkaError
from flask import Flask, request, jsonify


# Set up config, scheduler and logger
logging.basicConfig()
logger = logging.getLogger('kafka_producer')
logger.setLevel(logging.DEBUG)

config = ConfigParser.ConfigParser()
config.read('kafka_producer.cfg')

# Set up scheduler
schedule = BackgroundScheduler()
schedule.add_executor('threadpool')
schedule.start()

broker_ip = config.get('kafka', 'broker_ip')
posts_file = config.get('io', 'posts_file')
kafka_topic = config.get('kafka', 'topic')
flask_host = config.get('flask', 'host')
flask_port = config.get('flask', 'port')
kafka_producer = None
posts = None
app = Flask(__name__)

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
		logger.warn('Fail to shut down kafka producer')

@app.route('/home', methods=['POST'])
def get_data():
	logger.debug('I got the request')
	# {"CreationDate": "CreationDate", "OwnerUserId": "OwnerUserId", "ClosedDate": "ClosedDate", "Id": "Id", "Body": "Body", "Title": "Title", "Score": "Score"}
	data = request.form
	if not data:
		return 400
	# data : {"posts":posts, "id": id, "userid": userid}
	kafka_data = {}
	kafka_data["creationDate"] = time.time()
	kafka_data["OwnerUserId"] = data["userid"]
	kafka_data["ClosedDate"] = "NA"
	kafka_data["Id"] = data["id"]
	kafka_data["Body"] = data["posts"]
	kafka_data["Title"] = "NA"
	kafka_data["Score"] = "NA"
	output_data = json.dumps(kafka_data)
	send_to_broker(kafka_producer, output_data)
	return 200

# Simulate writing data to kafka broker
def generate_data():
	while True:
		for line in posts:
			send_to_broker(kafka_producer, line,)
			time.sleep(5)

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
	schedule.add_job(generate_data)
	
	# Start flask application
	app.run(host=flask_host, port=flask_port)
