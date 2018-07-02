from flask import render_template
from flask_app import app
from flask_cassandra import CassandraCluster


cassandra = CassandraCluster()
app.config['CASSANDRA_NODES'] = ['ec2-18-233-215-146.compute-1.amazonaws.com','ec2-34-234-45-181.compute-1.amazonaws.com','ec2-52-20-107-122.compute-1.amazonaws.com']  # list of nodes

@app.route("/cassandra_test")
def cassandra_test():
       session = cassandra.connect()
       session.set_keyspace("users")
       cql = "SELECT * FROM users.locations"
       r = session.execute(cql)
       return str(r[:2])

@app.route('/')
@app.route('/index')
def index():
       user = { 'nickname': 'Miguel' } # fake user
       return render_template("index.html", title="Home", user=user)
