from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from haversine import haversine

from cassandra.cluster import Cluster
from cass_man import Cassandra_Manager
from pyspark_cassandra import CassandraSparkContext, streaming
import pyspark_cassandra

import json

cassandra_node_IP = "ec2-18-233-215-146.compute-1.amazonaws.com"

# configure file for the cassandra context
conf = SparkConf() \
    .setAppName("proximity_secure") \
    .setMaster('local[*]') \
    .set("spark.cassandra.connection.host", cassandra_node_IP)

sc = CassandraSparkContext(conf = conf)
sc.setLogLevel("WARN")

# Create streaming context with mini-batch interval of 2 second
ssc = StreamingContext(sc, 2)

# Listens to kafka topic COMBO from one of the nodes in the kafka cluster
dstream_combo = KafkaUtils.createDirectStream(ssc, ['COMBO'], {"bootstrap.servers": 'ec2-52-0-13-173.compute-1.amazonaws.com:9092'})

# load json data from stream
json_combo  = dstream_combo.map(lambda x: json.loads(x[1]))
# calculate distance between locations
json_dist = json_combo.map(lambda data:distance(data))
json_dist.saveToCassandra('users','locations')

# filter out the values that should be flagged
flagged = json_dist.filter(lambda json:json['distance']>1.5)
flagged.saveToCassandra('users','flagged')

json_dist.pprint(1)

# function to find the distance between locations in json
def distance(json_data, miles=True):
    def extract_coords(topic_name):
        gps = json.loads(json_data[topic_name])
        json_data[topic_name] = gps
        lng = gps['lng']
        lat = gps['lat']

        return [lng,lat]

    loc_1 = extract_coords('PHONE_LOC')
    loc_2 = extract_coords('TRANS_LOC')
    distance = haversine(loc_1, loc_2, miles=miles)
    json_data['distance'] = distance
    
    return json_data

ssc.start()
ssc.awaitTermination()
