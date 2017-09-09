# Perform batch process to generate a classfication model
# Extract TF-IDF features using spark and then train naive bayes classifier to do classification
import logging
import ConfigParser

from pyspark import SparkContext, SparkConf
from pyspark import 


logging.basicConfig()
logger=logging.getLogger('model_generation')
logger.setLevel(logging.DEBUG)

config=ConfigParser.ConfigParser()
config.read('model_generation.cfg')

master=config.get('spark','master')
inputfile=config.get('io', 'inputfile')

if __name__ == '__main__':

	# Try to initialize a spark cluster with master, master can be local or mesos URL, which is configurable in config file 
	try:
		logger.debug("Initializing Spark cluster")
		conf=SparkConf()
		conf.setAppName('model_generation').setMaster(master)
		sc=SparkContext(conf=conf)
		logger.debug("Created Spark cluster successfully")
	except:
		logger.error("Fail to initialize spark cluster")

	# Input the dataset
	try:
		logger.debug("Start to read the input dataset")
		tagsRDD=sc.textfile(inputfile)
		




