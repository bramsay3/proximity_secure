from cassandra.cluster import Cluster


class Cassandra_Manager():
    """ This class is in charge of running commands to create tables 
        and keysapce in the cassandra table.
    """

    def __init__(self, cluser_IP=['ec2-18-233-215-146.compute-1.amazonaws.com']):
        cluster = Cluster(cluser_IP)
        self.session = cluster.connect()
        self.keyspace_name = None
        self.table_name = None

    def is_str(self, value):
        given_type = type(value)
        if given_type is not str:
            raise TypeError('Expected ' + str(value) + ' to be type string but was given type: ' +\
                            str(given_type))

    def set_keyspace(self, keyspace_name):
        self.is_str(keyspace_name)
        self.keyspace_name = keyspace_name
        self.session.execute('USE ' + keyspace_name)
        print('Using Keyspace:  ' + keyspace_name)

    def set_table(self, table_name):
        self.is_str(table_name)
        self.table_name = table_name
        print('Using Table:  ' + table_name)


    def insert(self, json_data_string):
        if self.table_name is None:
            raise Exception("The table to insert to is not yet defined. Consider set_table() or create_table")
        insert_query = "INSERT INTO " + self.table_name + "JSON" + json_data_string

        self.session.execute(insert_query)
        print("inserted a JSON")



    def create_keyspace(self, keyspace_name = 'users'):
        self.is_str(keyspace_name)
        make_keyspace = "CREATE KEYSPACE IF NOT EXISTS " + keyspace_name + " WITH " +\
                        "replication = {'class':'SimpleStrategy', 'replication_factor':3}"

        self.session.execute(make_keyspace)
        self.session.execute('USE ' + keyspace_name)
        self.set_keyspace(keyspace_name)

    def create_type(self):

        make_type = "CREATE TYPE IF NOT EXISTS " + "gps_loc" +\
                     "(lng float, " + \
                     "lat float);"

        self.session.execute(make_type)

    def create_table(self, table_name = 'locations'):
        self.is_str(table_name)
        make_table = "CREATE TABLE IF NOT EXISTS " + table_name + \
                     "(\"USER_ID\" int, " + \
                     "\"TRANSACTIONS_TIMESTAMP\" timestamp, " +\
                     "distance float, " +\
                     "\"PHONE_LOC\" map<text, float>, " +\
                     "\"TRANSACTION_LOC\" map<text, float>, " +\
                     "\"PHONE_TIMESTAMP\" timestamp, " +\
                     "PRIMARY KEY (\"USER_ID\", \"TRANSACTIONS_TIMESTAMP\"));"

        self.session.execute(make_table)
        self.set_table(table_name)



