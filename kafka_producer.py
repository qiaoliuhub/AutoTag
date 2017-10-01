import logging
import ConfigParser
import atexit
import time
import json
import tornado.ioloop
import tornado.web
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from kafka import KafkaProducer
from kafka.errors import KafkaError


# Set up config, scheduler and logger
logging.basicConfig()
logger = logging.getLogger('kafka_producer')
logger.setLevel(logging.DEBUG)

config = ConfigParser.ConfigParser()
config.read('kafka_producer.cfg')

# Set up scheduler and default ThreadPoolExecutor has a default maximum thread count of 10
schedule = BackgroundScheduler()
schedule.add_executor('threadpool')
schedule.start()

broker_ip = config.get('kafka', 'broker_ip')
posts_file = config.get('io', 'posts_file')
kafka_topic = config.get('kafka', 'topic')
tornado_host = config.get('tornado', 'host')
tornado_port = config.get('tornado', 'port')
kafka_producer = None
posts = None

class posts_handler(tornado.web.RequestHandler):
	def post(self):
		data = self.request.arguments
		if not data:
			return
		# data : {"posts":posts, "id": id, "userid": userid}
		kafka_data = {}
		kafka_data["CreationDate"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
		kafka_data["OwnerUserId"] = data["userid"][0]
		kafka_data["ClosedDate"] = "NA"
		kafka_data["Id"] = data["id"][0]
		kafka_data["Body"] = data["posts"][0]
		kafka_data["Title"] = "NA"
		kafka_data["Score"] = "NA"
		output_data = json.dumps(kafka_data)
		send_to_broker(kafka_producer, output_data)
		logger.debug("Send data %s successfully" %output_data)

def tornado_app():
    return tornado.web.Application([
        (r"/home", posts_handler),
    ])

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
		logger.debug('Stopping tornado application')
		tornado.ioloop.IOLoop.instance().stop()
		logger.debug('Stopped tornado appliation successfully')
	except:
		logger.debug('Fail to stop tornado applicaiton')

	try:
		logger.debug('Try to shut down kafka producer')
		kafka_producer.flush()
		kafka_producer.close()
		logger.debug('Shut down kafka producer successfully')
	except KafkaError as ke:
		logger.warn('Fail to shut down kafka producer, caused by %s' % ke.message)

	try:
		logger.debug('Shuting down scheduler')
		schedule.shutdown()
		logger.debug('Shut down scheduler successfully')
	except Exception as e:
		logger.debug('Fail to shut down scheduler')


# Simulate writing data to kafka broker
def generate_data():
	while True:
		for line in posts:
			send_to_broker(kafka_producer, line,)
			time.sleep(2)

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

	# Set up tornado application
	app = tornado_app()
	app.listen(tornado_port, address=tornado_host)

	atexit.register(shutdown_hook, posts, kafka_producer)

	# Sending data to broker
	schedule.add_job(generate_data)
	
	# Start tornado application
	tornado.ioloop.IOLoop.current().start()
