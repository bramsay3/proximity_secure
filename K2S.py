
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


conf = SparkConf()
sc = SparkContext(master = 'local[*]', appName="proximity_solver", conf = conf)
sc.setLogLevel("WARN")
#Create streaming context with mini-batch interval of 1 second
ssc = StreamingContext(sc, 1)

kvs = KafkaUtils.createDirectStream(ssc, ['phone_loc'], {"bootstrap.servers": 'localhost:9092'})

lines = kvs.map(lambda x: x[1])
counts = lines.flatMap(lambda line: line.split(" ")) \
              .map(lambda word: (word, 1)) \
              .reduceByKey(lambda a, b: a+b)

counts.pprint()


offsetRanges = []

def storeOffsetRanges(rdd):
    global offsetRanges
    offsetRanges = rdd.offsetRanges()
    return rdd

def printOffsetRanges(rdd):
    for o in offsetRanges:
        print("%s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset))
"""
kvs \
 .transform(storeOffsetRanges) \
 .foreachRDD(printOffsetRanges)
"""
ssc.start()
ssc.awaitTermination()
