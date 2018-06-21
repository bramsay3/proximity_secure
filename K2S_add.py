from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import json



conf = SparkConf()
sc = SparkContext(master = 'local[*]', appName="proximity_solver", conf = conf)
sc.setLogLevel("WARN")
sc.addFile("/home/ubuntu/.local/lib/python3.5/site-packages/haversine/__init__.py")
from __init__ import haversine

#Create streaming context with mini-batch interval of 1 second
ssc = StreamingContext(sc, 1)

dstream_combo = KafkaUtils.createDirectStream(ssc, ['COMBO'], {"bootstrap.servers": 'localhost:9092'})

json_combo  = dstream_combo.map(lambda x: json.loads(x[1]))

json_dist = json_combo.map(lambda data:distance(data)) #\
    #        .filter(lambda dist:dist>200)

json_dist.pprint(100)


def distance(json_data, miles=True):
    def extract_coords(topic_name):
        gps = json.loads(json_data[topic_name])
        lng = gps['lng']
        lat = gps['lat']

        return [lng,lat]

    loc_1 = extract_coords('PHONE_LOC')
    loc_2 = extract_coords('TRANSACTION_LOC')
    distance = haversine(loc_1, loc_2, miles=miles)

    json_data['distance'] = distance
    
    return json_data









#json_combo.pprint()


ssc.start()
ssc.awaitTermination()
