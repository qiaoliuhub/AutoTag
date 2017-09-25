import logging
import ConfigParser
import atexit
import json

from kafka import KafkaConsumer
from cassandra.cluster import Cluster

logging.basicConfig()
logger = logging.getLogger('persist_data')
logger.setLevel(logging.DEBUG)

config = ConfigParser.ConfigParser()
config.read('persist_data.cfg')

kafka_topic = config.get('kafka', 'topic_tag')
kafka_broker = config.get('kafka', 'broker')
contact_points = config.get('cassandra', 'contact_points')
keyspace = config.get('cassandra', 'keyspace')
table = config.get('cassandra', 'table')

def persist_data(post, cassandra_session):
	try:
		parsed_post = json.loads(post)
		logger.debug("Start to parse the post data type :%s, %s" % (type(parsed_post), parsed_post))
		creation_date = parsed_post.get("CreationDate")
		owner_userid = parsed_post.get("OwnerUserId")
		closed_date = parsed_post.get("ClosedDate")
		post_id = parsed_post.get("Id")
		body = parsed_post.get("Body")
		title = parsed_post.get("Title")
		score = parsed_post.get("Score")
		tag = parsed_post.get("tags")
		logger.debug("Parse data successfully haha %s" % post_id)
		insert_data = "INSERT INTO %s (Id, Title, Body, OwnerUserId, CreationDate, ClosedDate, Score, tag)\
		 VALUES (?,?,?,?,?,?,?,?)" % table
		logger.debug("start to send to cassandra")
		prepared = cassandra_session.prepare(insert_data)
		cassandra_session.execute(prepared, (post_id, title, body, owner_userid, creation_date, closed_date, score, tag))
		logger.debug("Persist data successfully")
	except Exception as e:
		logger.debug("Fail to persist data, caused by %s" %e.message)


def shutdown_hook(cassandra_session, kafka_consumer):
	try:
		logger.debug("disconnect cassandra session")
		cassandra_session.shutdown()
		logger.debug("disconnect cassandra successfully")
	except:
		logger.debug("Fail to disconnect cassandra session")

	try:
		logger.debug("Stop kafka consumer")
		kafka_consumer.close()
		logger.debug("stop kafka consumer successfully")
	except:
		logger.debug("Fail to stop kafka Consumer")

if __name__ == '__main__':
	
	# Initiate a kafka consumer
	try:
		logger.debug("Initiating kafka consumer")
		kafka_consumer = KafkaConsumer(kafka_topic, bootstrap_servers=kafka_broker)
		logger.debug("Start kafka consumer successfully")
	except:
		logger.debug("Fail to start kafka consumer")

	# Initiate cassandra session
	try:
		logger.debug("Start to connect to cassandra cluster")
		cassandra_cluster = Cluster(contact_points=contact_points.split(","))
		cassandra_session = cassandra_cluster.connect()
		logger.debug("Connect to cassandra session successfully")
	except:
		logger.debug("Fail to connect to cassandra session")

	# Statement used to create a keyspace and table
	# {"CreationDate": "CreationDate", "OwnerUserId": "OwnerUserId", "ClosedDate": "ClosedDate", "Id": "Id", "Body": "Body", "Title": "Title", "Score": "Score"}
	create_keyspace = "CREATE KEYSPACE IF NOT EXISTS %s with replication = {'class' : 'SimpleStrategy', 'replication_factor' : '1'} and durable_writes ='true' " % keyspace
	create_table = "CREATE TABLE IF NOT EXISTS %s (Id text, Title text, Body text, OwnerUserId text, CreationDate text, ClosedDate text, Score text, tag text, PRIMARY KEY (Id))" % table
	drop_table ="DROP TABLE IF EXISTS %s.%s" % (keyspace, table)
	# CREAT KEYSPACE, USE THE KEYSPACE AND THEN CREATE THE TBALE
	try:
		logger.debug("create the key space and use it")
		cassandra_session.execute(create_keyspace)
		cassandra_session.set_keyspace(keyspace)
		logger.debug("create keyspace successfully, start to create table in %s" %keyspace)
		cassandra_session.execute(drop_table)
		cassandra_session.execute(create_table)
		logger.debug("Create table successfully")
	except:
		logger.debug("Fail to create keyspace or table")

	# Register shutdown hook
	atexit.register(shutdown_hook, cassandra_session, kafka_consumer)

	 	# Consume data from kafka broker
 	for post in kafka_consumer:
 		persist_data(post.value, cassandra_session)

