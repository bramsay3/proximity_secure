from cassandra.cluster import Cluster
from cassandra.query import *

cluster = Cluster(['ec2-18-233-215-146.compute-1.amazonaws.com'])
session = cluster.connect()
session.row_factory =  dict_factory
cql = "SELECT * FROM users.locations LIMIT 10"
results = session.execute(cql)


def stringify_query(row):
    def stringify_loc(location_json):
        lng = location_json['lng']
        lat = location_json['lat']
        string = 'Lat: {}   Lng: {}'.format(lng,lat)
        return string

    phone_loc_str = stringify_loc(row['PHONE_LOC'])
    trans_loc_str = stringify_loc(row['TRANS_LOC'])
    print(row)
    row['PHONE_LOC'] = phone_loc_str
    row['TRANS_LOC'] = trans_loc_str
    return row


print(list(map(lambda row:stringify_query(row),results)))




#for row in results:
#    print(round(row.PHONE_LOC['lng'],2))
