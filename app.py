import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify
from flask import request

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station


from flask import Flask, jsonify



#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# List all the stations 
#################################################

@app.route("/api/v1.0/stations")
def stations():
    """Return a list of all station """
    # Query all stations
    session = Session(engine)
    results = session.query(Station.name).all()

    # Create a dictionary from the row data and append to a list
    station_names = []
    for station in results:
        stat_dict ={}
        stat_dict["Station"] = station
        station_names.append(stat_dict)
    return jsonify(station_names)

#################################################
# Query for the dates and temperature observations 
# from a year from the last data point.
#################################################
@app.route("/api/v1.0/tobs")
def tobs():
    
    session = Session(engine)
    active_station = active_stations()
    active_station = active_station.get('station')
    active_station
    sel_start_date = [Measurement.date]

    start_date = session.query(*sel_start_date).\
                filter(Measurement.station == active_station).\
                order_by(Measurement.date.desc()).first()
    start_date = pd.to_datetime(start_date, format='%Y-%m-%d')

    end_date = start_date - timedelta(days = 365)
    end_date = end_date.date[0]
    last_12_temp = session.query(Measurement.date ,Measurement.tobs).\
                    filter(Measurement.date >= end_date ).\
                    filter(Measurement.station == active_station).\
                    order_by(Measurement.date.desc()).all()
    last_12_temp_list = list(np.ravel(last_12_temp))


    return jsonify(last_12_temp_list)

#################################################
# For start date only and and start and end date
#################################################
#   
@app.route("/api/v1.0/dates" , methods=['GET', 'POST'])
def dates_stats():
    session = Session(engine)
    start_date = request.args.get('startDate')
    
    print(f' start date : {start_date}')
    
    if(request.args.get('endDate')):
        print('Present')
        end_date = request.args.get('endDate')
        start_date_year_ago = date_year_ago(start_date)
        end_date_year_ago = date_year_ago(end_date)
        print(f'start date year ago {start_date_year_ago}')
        print(f'end date a year ago {end_date_year_ago}')
        stat_data = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
                filter(Measurement.date >= start_date_year_ago).\
                filter(Measurement.date <= end_date_year_ago).all()
        return jsonify(stat_data)
    # if only start date param present in the url
    else:
        print(start_date)
        received_date = date_year_ago(start_date)
        max_temp = [func.max(Measurement.tobs),

        func.min(Measurement.tobs),
        func.avg(Measurement.tobs)]
        stat_data = session.query(*max_temp).\
            filter(Measurement.date >= received_date).all()
        print(type(received_date))
        return jsonify(stat_data)
     
    return jsonify(start_date)

#################################################
# Calculates stats for travel date between start 
# and end date a year ago
#################################################
#  

@app.route("/api/v1.0/traveldates" , methods=['GET', 'POST'])
def travel():
    session = Session(engine)
    start_date = request.args.get('startDate')
    if(request.args.get('endDate')):
        print('Present')
        end_date = request.args.get('endDate')
    print(f' start date : {start_date}')
    print(f' end date : {end_date}')
    
    start_date_year_ago = date_year_ago(start_date)
    end_date_year_ago = date_year_ago(end_date)
    print(f'start date year ago {start_date_year_ago}')
    print(f'end date a year ago {end_date_year_ago}')

    stat_data = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date_year_ago).filter(Measurement.date <= end_date_year_ago).all()
    return jsonify(stat_data)
    
#################################################
# Calculates stats for any date  and greter
#################################################
 

@app.route("/api/v1.0/temp", methods=['GET', 'POST'])
def user_input():
    session = Session(engine)
    startDate = request.args.get('requestDate')
    
    print(startDate)
    received_date = date_year_ago(startDate)
    max_temp = [func.max(Measurement.tobs),

           func.min(Measurement.tobs),
           func.avg(Measurement.tobs)]
    stat_data = session.query(*max_temp).\
         filter(Measurement.date >= received_date).all()
    print(type(received_date))
    return jsonify(stat_data)

#################################################
# Convert the query results to a Dictionary using 
# date as the key and prcp as the value.
#################################################

@app.route("/api/v1.0/precipitation")
def precipitation():
    sel = [Measurement.date, 
       Measurement.prcp
       
      ]

    session = Session(engine)
    result = session.query(*sel).group_by(Measurement.date).all()
    precipitation = []
    for date, prcp in result:
         precipitation_dict ={}
         precipitation_dict["Date"] = date
         precipitation_dict["Precipitation"] = prcp
         precipitation.append(precipitation_dict)
    return jsonify(precipitation)

#################################################
# Home page
#################################################

@app.route("/")
def welcome():
    return (
        f"Welcome to the JSurfs Up API!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation <br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<br/>"
        f"/api/v1.0/&lt;start&gt;/&lt;end&gt;<br />"
    )

#################################################
# Reusable code
#################################################

def active_stations():
    session = Session(engine)
    sel_active_stations = [Measurement.station,
                      func.count(Measurement.station)]
    active_station = session.query(*sel_active_stations).\
                        group_by(Measurement.station).\
                        order_by( func.count(Measurement.station).desc()).first()
    
  
    active_station = active_station._asdict()
    
    return active_station
def date_year_ago(received_date):
    print(received_date)
    received_date = datetime.strptime(received_date, '%Y-%m-%d')
    received_date = received_date.date()
    start_date_year_ago = received_date - timedelta(days = 365)
    print(start_date_year_ago)
    return start_date_year_ago
   

if __name__ == "__main__":
    app.run(debug=True)
