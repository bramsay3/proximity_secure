from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from haversine import haversine
from cassandra.cluster import Cluster
from cass_man import Cassandra_Manager


import json



conf = SparkConf()
sc = SparkContext(master = 'local[*]', appName="proximity_solver", conf = conf)
sc.setLogLevel("WARN")

manager = Cassandra_Manager()
manager.create_keyspace()
manager.create_table()

#Create streaming context with mini-batch interval of 1 second
ssc = StreamingContext(sc, 1)

dstream_combo = KafkaUtils.createDirectStream(ssc, ['COMBO'], {"bootstrap.servers": 'localhost:9092'})

json_combo  = dstream_combo.map(lambda x: json.loads(x[1]))
json_dist = json_combo.map(lambda data:distance(data)) 
json_list = json_dist.foreachRDD(lambda rdd: rdd.collect())
for user in json_list:
    manager.insert(user)

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

ssc.start()
ssc.awaitTermination()
