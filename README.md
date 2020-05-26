# Real-Time-Tweet-Analysis-with-Spark-Streaming

## Overview of Spark Streaming

Spark Streaming is an extension of the core Spark API that enables scalable, high-throughput, fault-tolerant stream processing of live data streams. Data can be ingested from many sources like Kafka, Flume, Kinesis, or TCP sockets, and can be processed using complex algorithms expressed with high-level functions like map, reduce, join and window. Finally, processed data can be pushed out to filesystems, databases, and live dashboards. 

![Spark Streaming](https://spark.apache.org/docs/latest/img/streaming-arch.png)

Internally, it works as follows. Spark Streaming receives live input data streams and divides the data into batches, which are then processed by the Spark engine to generate the final stream of results in batches.

![Spark Streaming in Batches](https://spark.apache.org/docs/latest/img/streaming-flow.png)

[Source Link](https://spark.apache.org/docs/latest/streaming-programming-guide.html)

## Objective of this Project

The project deals with extracting and analyzing insights from social network data in real time using one of the most important big data solutions — Apache Spark and Python. In this project, I have build a simple application that reads online tweets in the form of streams from Twitter using Tweepy in Python, & then processes the tweets using Apache Spark with Streaming to identify hashtags and, finally, returns top trending hashtags. It also returns the sentiments behind the incoming tweets.

*Software & Working with files* :

I am running this app on an Ubuntu Vm inside Virtual Box. Additionally, I have installed Python(and the required libraries), Java, Scala & Apache Spark.

* TweetStream.py - This file is for extracting streams of tweets and routing them through a Stream Listener to a secure socket connection. I have hidden the API & Token keys for accessing the Twitter API  
* SparkStreamingApp.ipynb - In this file, Spark Streaming consumes the data streams and feeds it to the Spark engine for processing and analyzing the tweets.
