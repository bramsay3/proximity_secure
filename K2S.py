
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


conf = SparkConf()
sc = SparkContext(master = 'local[*]', appName="proximity_solver", conf = conf)
sc.setLogLevel("WARN")
#Create streaming context with mini-batch interval of 1 second
ssc = StreamingContext(sc, 1)

directKafkaStream = KafkaUtils.createDirectStream(ssc, ['phone_loc'], {"bootstrap.servers": 'localhost:9092'})

offsetRanges = []

def storeOffsetRanges(rdd):
    global offsetRanges
    offsetRanges = rdd.offsetRanges()
    return rdd

def printOffsetRanges(rdd):
    for o in offsetRanges:
        print("%s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset))

directKafkaStream \
 .transform(storeOffsetRanges) \
 .foreachRDD(printOffsetRanges)
