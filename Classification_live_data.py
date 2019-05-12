import json
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import socket
from pyspark.sql import SQLContext
from pyspark.sql import Row
import sys
import requests
import shutil
import nltk
from nltk.tokenize import word_tokenize
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from nltk.classify import ClassifierI
from statistics import mode

def send_df_to_dashboard(df, analysis_type):
    if analysis_type == 'sentiment_analysis':
        sentiment = [str(t.sentiment) for t in df.select("sentiment").collect()]
        frequency = [p.frequency for p in df.select("frequency").collect()]
        url = 'http://localhost:5002/updateData'
        request_data = {'label': str(sentiment), 'data': str(frequency)}
    else:
        top_tags = [str(t.hashtag) for t in df.select("hashtag").collect()]
        tags_count = [p.hashtag_count for p in df.select("hashtag_count").collect()]
        url = 'http://localhost:5002/update_hashtag_data'
        request_data = {'label': str(top_tags), 'data': str(tags_count)}
    print(request_data)
    response = requests.post(url, data=request_data)

def classify(transformer):
    v = NB_load_model.predict(transformer)
    return v

def get_sql_context_instance(spark_context):
    return SQLContext(spark_context)

def aggregate_tags_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

def process_rdd(rdd):
    try:
        sql_context = get_sql_context_instance(rdd.context)
        #transformed_rdd = rdd.map(lambda x: 'positive' if x[1]==0 else '')
        row_rdd = rdd.map(lambda w: Row(sentiment=w[0], frequency=w[1]))
        hashtags_df = sql_context.createDataFrame(row_rdd)
        #print(hashtags_df.collect())
        hashtags_df.registerTempTable("sentiments")
        hashtag_counts_df = sql_context.sql("select sentiment, frequency FROM sentiments")
        hashtag_counts_df.show()
        #print(hashtag_counts_df.count())
        analysis_type = 'sentiment_analysis'
        send_df_to_dashboard(hashtag_counts_df, analysis_type)
    except:
        pass

def hashtag_process(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
        hashtags_df = sql_context.createDataFrame(row_rdd)
        hashtags_df.registerTempTable("hashtags")
        hashtag_counts_df = sql_context.sql("select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 10")
        #print(hashtag_counts_df.show())
        analysis_type = 'hashtag_analysis'
        send_df_to_dashboard(hashtag_counts_df,analysis_type)
    except:
        e = sys.exc_info()[0]
        print("There is an error: %s" % e)


conf = SparkConf()
conf.setAppName("TwitterStreamApp")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc,3)
ssc.checkpoint("checkpoint_models")
htf = HashingTF(50000)
NB_output_dir = '/spark/NaiveBayes'
NB_load_model= NaiveBayesModel.load(sc, NB_output_dir)


# Sentiment Analysis #

## 01 read tweets from stream ##
dataStream = ssc.socketTextStream("localhost",9009)
## 02 split the text into words #
words = dataStream.map(lambda x: x.split(" "))
## 03 transformed the words into features ##
features = words.map(lambda x: htf.transform(x))
## 04 predict the sentiment ##
prediction = features.map(lambda x: classify(x))
## 05 label the sentiments ##
label_sentiments = prediction.map(lambda x: ('positive',1) if x==1 else ('negative',1))
## 06 aggregate the results using sentiment as key ##
results = label_sentiments.updateStateByKey(aggregate_tags_count)

# Hashtag Analysis #
## 01 split each tweet into words
tweets = dataStream.flatMap(lambda line: line.split(" "))
## 02 filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
hashtags = tweets.filter(lambda w: '#' in w).map(lambda x: (x, 1))
# 03 adding the count of each hashtag to its last count
tags_totals = hashtags.updateStateByKey(aggregate_tags_count)


results.foreachRDD(lambda r : process_rdd(r))
tags_totals.foreachRDD(hashtag_process)
ssc.start()
ssc.awaitTermination()
