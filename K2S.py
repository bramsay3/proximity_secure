
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json


conf = SparkConf()
sc = SparkContext(master = 'local[*]', appName="proximity_solver", conf = conf)
sc.setLogLevel("WARN")
#Create streaming context with mini-batch interval of 1 second
ssc = StreamingContext(sc, 1)

dstream_combo = KafkaUtils.createDirectStream(ssc, ['COMBO'], {"bootstrap.servers": 'localhost:9092'})

json_combo  = dstream_combo.map(lambda x: json.loads(x[1]))

phone_loc = json_combo.map(lambda data:data['PHONE_LOC']) \
            .map(lambda loc:json.loads(loc)) \
            .map(lambda data:(data['lng'],1)) \
            .reduceByKey(lambda v,k:v+k)



phone_loc.pprint()





#json_combo.pprint()


ssc.start()
ssc.awaitTermination()
