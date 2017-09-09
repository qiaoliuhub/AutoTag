# Autotagging Overflow
The objective of this pipeline is to enable autotagging of stack overflow questions and answers. The model will be trained with TF-IDF with Spark MLlib in batch processing. Then the model would be used to autotag the new coming questions from Kafka (latency?). Finally the data will be persisted in Cassandra to support the front end.

# Sample datasets

<p align="center">
  <img src="/sample_dataset.png" width="900"/>
</p>

# Tag
<p align="center">
  <img src="/tag.png" width="900"/>
</p>

# Pipeline
<p align="center">
  <img src="/pipeline.png" width="900"/>
</p>

# Explore the data set

## Feature Extraction for each answer or questions

Use TF-IDF to form a vector for each questions or answers:
1. TF(term frequency) is the frequency of a word appears in a document
2. IDF(inverted document frequency) is a measurement of whether a word is common or rare in the whole documents

<p align="center">
  <img src="/data_exlporation.png" width="900"/>
</p>

# Batch process (model_generation.py)
1. Extract text features using TF-IDF
2. Train a Naive bayes classifier to do multiclass classfication 

