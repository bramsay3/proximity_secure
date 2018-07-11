from flask import render_template, request
from flask_app import app
from cassandra.cluster import Cluster
from cassandra.query import *


@app.route("/map")
def map():
    return render_template("map.html")

@app.route('/')
@app.route("/table", methods=['GET','POST'])
def table():
    cluster = Cluster(['ec2-18-233-215-146.compute-1.amazonaws.com'], load_balancing_policy=cassandra.policies.RoundRobinPolicy())
    session = cluster.connect()
    #       session = cluster.connect(['ec2-18-233-215-146.compute-1.amazonaws.com','ec2-34-234-45-181.compute-1.amazonaws.com','ec2-52-20-107-122.compute-1.amazonaws.com'])
    session.row_factory =  dict_factory

    if request.method is 'POST':
        #request.form.get()
        print('here')

    else:
        print('else')
    cql = "SELECT * FROM users.flagged"
    results = session.execute(cql)

    session.shutdown()
    cluster.shutdown()
    
    def stringify_query(row):
       def stringify_loc(location_json):
                  lng = location_json['lng']
                  lat = location_json['lat']
                  string = 'Lat: {}   Lng: {}'.format(round(lng,2),round(lat,2))
                  return string

       phone_loc_str = stringify_loc(row['PHONE_LOC'])
       phone_date = row['PHONE_TIMESTAMP']
       trans_loc_str = stringify_loc(row['TRANS_LOC'])
       trans_date = row['TRANSACTIONS_TIMESTAMP']
       distance = round(row['distance'],2)
       row['PHONE_TIMESTAMP'] = phone_date.strftime("%B %d, %Y %I:%M%p")
       row['TRANSACTIONS_TIMESTAMP'] = trans_date.strftime("%B %d, %Y %I:%M%p")
       row['PHONE_LOC'] = phone_loc_str
       row['TRANS_LOC'] = trans_loc_str
       row['distance'] = distance
       return row

    rows = [stringify_query(row) for row in results]

    header = ['User ID','Transaction Location','Transaction Time','Phone Location','Phone Time','Distance(miles)']
    return render_template("temp.html", header=header, rows = rows)



