 
from __future__ import print_function

import sys
from datetime import datetime
from random import randint
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from datetime import datetime
from elasticsearch import Elasticsearch

es = Elasticsearch()
locations=[{"lat": 32.8209296,"lon":-97.0115336},{"lat":33.6056711,"lon":-112.4052446},{"lat":40.7058316,"lon":-74.2581934},{"lat":47.6149942,"lon":-122.4759903}]

def es(rdd):
    for x in rdd.collect():
        tweetEntry={}
        tweetEntry['text']=x
        senti=sid.polarity_scores(x)['compound']
        if senti >0:
            tweetEntry['postive']=1
        elif senti<0:
            tweetEntry['negative']=1
        else:
            tweetEntry['neutral']=1  
        tweetEntry['timestamp']= datetime.now()
        tweetEntry['location']=locations[randint(0,3)]
        es.index(index=“xyz”, doc_type="test-type", body=tweetEntry)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: consume.py <host> <topic>", file=sys.stderr)
        exit(-1)
    sid = SentimentIntensityAnalyzer()
    sc = SparkContext()
    ssc = StreamingContext(sc, 1)
    host, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, host, "spark-streaming-consumer", {topic: 1})
    lines = kvs.map(lambda x: x[1].encode("ascii","ignore"))
    lines.foreachRDD(es)
    ssc.start()
    ssc.awaitTermination()