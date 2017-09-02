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
