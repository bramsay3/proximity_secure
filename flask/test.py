from cassandra.cluster import Cluster
from cassandra.query import *

cluster = Cluster(['ec2-18-233-215-146.compute-1.amazonaws.com'])
session = cluster.connect()
session.row_factory =  named_tuple_factory
cql = "SELECT * FROM users.locations LIMIT 10"
results = session.execute(cql)
print(results[:2])
#for row in results:
#    print(round(row.PHONE_LOC['lng'],2))
