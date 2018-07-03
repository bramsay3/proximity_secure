from flask import render_template
from flask_app import app
from cassandra.cluster import Cluster
from cassandra.query import *
from flask_table import Table, Col


class ItemTable(Table):
           USER_ID = Col('user_ID')
           TRANSACTIONS_TIMESTAMP = Col('trans_time')
           PHONE_LOC = Col('phone_loc')
           PHONE_TIMESTAMP = Col('phone_time')
           TRANS_LOC = Col('trans_loc')
           distance = Col('distance')

@app.route("/table")
def table():
       cluster = Cluster(['ec2-18-233-215-146.compute-1.amazonaws.com'])
       session = cluster.connect()
#       session = cluster.connect(['ec2-18-233-215-146.compute-1.amazonaws.com','ec2-34-234-45-181.compute-1.amazonaws.com','ec2-52-20-107-122.compute-1.amazonaws.com'])
       session.row_factory =  named_tuple_factory
       cql = "SELECT * FROM users.locations LIMIT 10"
       results = session.execute(cql)
       

       header = ['User ID','Transaction Location','Transaction Time','Phone Location','Phone Time','Distance(miles)']
           
       return render_template("Table_Fixed_Header/temp.html", header=header, rows = results)



@app.route('/')
@app.route('/index')
def index():
       user = { 'nickname': 'Miguel' } # fake user
       return render_template("index.html", title="Home", user=user)
