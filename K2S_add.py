from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from haversine import haversine
from cassandra.cluster import Cluster



import json



conf = SparkConf()
sc = SparkContext(master = 'local[*]', appName="proximity_solver", conf = conf)
sc.setLogLevel("WARN")


#Create streaming context with mini-batch interval of 1 second
ssc = StreamingContext(sc, 1)

dstream_combo = KafkaUtils.createDirectStream(ssc, ['COMBO'], {"bootstrap.servers": 'localhost:9092'})

json_combo  = dstream_combo.map(lambda x: json.loads(x[1]))
json_dist = json_combo.map(lambda data:distance(data)) #\
    #        .filter(lambda dist:dist>200)

json_dist.pprint(100)


cluster = Cluster()
session = cluster.connect(['18.233.215.146'])
create_keyspace(session)
create_table(session)



def create_keyspace(session, keyspace_name = 'user_data'):
    if type(keyspace_name) is not str:
        raise TypeError('keyspace_name must be of type string but was given type: '\
                        + str(type(keyspace_name)))
    
    make_keyspace = "CREATE KEYSPACE " + keyspace_name + " WITH " + \
                    "replication = {'class':'SimpleStrategy', 'replication_factor':3}"

    session.execute(make_keyspace)
    session.execute('USE ' + keyspace_name)
    print('Keyspace Created: ' + keyspace_name)

def create_table(session, table_name = 'user_locs'):
    if type(table_name) is not str:
        raise TypeError('table_name must be of type string but was given' +\
                        'type: '+ str(type(table_name)))

    make_table = "CREATE TABLE " + table_name + "(user_ID int PRIMARY KEY, " + \
                    "transaction_lat float, " +\
                    "transaction_lng flaot, " +\
                    "transaction_time, "  +\
                    "phone_lat float, " + \
                    "phone_lng float, " + \
                    "phone_time, " +\
                    "distance float)"

    session.execute(make_table)
    print('Table Created: ' + table_name)




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
