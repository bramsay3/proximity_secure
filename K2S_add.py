from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from haversine import haversine

from cassandra.cluster import Cluster
from cass_man import Cassandra_Manager
from pyspark_cassandra import CassandraSparkContext, streaming
import pyspark_cassandra


import json



conf = SparkConf() \
    .setAppName("proximity_secure") \
    .setMaster('local[*]') \
    .set("spark.cassandra.connection.host","ec2-18-233-215-146.compute-1.amazonaws.com")

sc = CassandraSparkContext(conf = conf)
sc.setLogLevel("WARN")


#Create streaming context with mini-batch interval of 1 second
ssc = StreamingContext(sc, 2)

dstream_combo = KafkaUtils.createDirectStream(ssc, ['COMBO'], {"bootstrap.servers": 'localhost:9092'})

#rdd = sc.parallelize([{"user_id": 9, "transactions_timestamp": "2019-07-14 22:00:00", "distance": 8109.8952838063815, "phone_loc": "{\"lat\":43.10041624519112,\"lng\":-104.52802039617467}", "TRANSACTION_LOC": "{\"lng\":39.83744810841391,\"lat\":-103.22536502244108}", "PHONE_TIMESTAMP": "2019-02-14 17:00:00"}])

#rdd = sc.parallelize([{'user_id': 3, 'transactions_timestamp': '2019-06-04 22:00:00', 'distance': 461.7505633584639, 'phone_loc': {"lat":40.602087555124854,"lng":-108.76210017318485}, 'transaction_loc': {"lat":39.550158835808006,"lng":-102.08468949015811}, 'phone_timestamp': '2019-02-14 17:00:00'}])

#rdd = sc.parallelize([{'word': 'me','count':5}])
#print(rdd.take(1))
#rdd.saveToCassandra('users','locations')
#print(rdd.take(1))

json_combo  = dstream_combo.map(lambda x: json.loads(x[1]))
json_dist = json_combo.map(lambda data:distance(data))
#json_dist.foreachRDD(lambda dic:dic.saveToCassandra('users','locations'))

#print(sc.cassandraTable("users","locations").select("phone_loc").take(3))
json_dist.pprint(100)
json_dist.saveToCassandra('users','locations')

#json_list = json_dist.foreachRDD(lambda rdd: rdd.collect())

#for user in json_list:
#    manager.insert(user)






def distance(json_data, miles=True):
    def extract_coords(topic_name):
        gps = json.loads(json_data[topic_name])
        json_data[topic_name] = gps
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
