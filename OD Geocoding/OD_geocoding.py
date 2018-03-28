# This script will get the Juarez & EP trips and will assign an Origin Census Zone and Destination Census Zone
import pyodbc
import config
import csv
import os.path
import requests
import mysql.connector as mariadb
import time
import sys
from multiprocessing.pool import Pool


rpt_db_cnxn = pyodbc.connect(config.RPTDB_CNXN)
rpt_db_cursor = rpt_db_cnxn.cursor()
pip_api = config.PIP_API
my_db_cnxn = mariadb.connect(host=config.MY_DB_URL, user=config.MY_DB_USER, passwd=config.MY_DB_PASSWORD, db=config.MY_DB_DB, port=3300)
my_db_cursor = my_db_cnxn.cursor()


def get_trips_from_db():
    # get's all trips that need zones
    query = """ SELECT TOP 100 MetropianID, MetropianID+1117 as random_id, routedistance, DistanceFromTrajectory, EstimateTravelTime, 
                actualMin5, localstarttime, localtripendtime, localhh, localweekday, LocalWW, localmm, localyyyy, startcity, 
                EndCity, StartLatitude, StartLongitude, EndLatitude, EndLongitude, TripSummaryID
				FROM  rpttripsummary
				WHERE  startcity <> EndCity and YEAR(localstarttime) >= 2017 and actualMin > 5 and DistanceFromTrajectory > 2
                and StartLatitude is not null and StartLongitude is not null and EndLongitude is not null and EndLatitude is not null
                ORDER BY TripSummaryID"""
    rpt_db_cursor.execute(query)
    data = rpt_db_cursor.fetchall()
    tripdata = []
    for row in data:
        lista = [int(row[19]), row[0], row[1], int(row[2]), int(row[3]), int(row[4]), int(row[5]), row[6], row[7], str(row[8]), str(row[9]), str(row[10]), 
        str(row[11]), str(row[12]), row[13], row[14], row[15], row[16], row[17], row[18]]
        tripdata.append(lista)

    return tripdata


def get_trips():
    # first check if we already have the data locally
    if os.path.isfile('trips.csv') == False:
        trips = get_trips_from_db()
        with open('trips.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(trips)
    else:
        trips = []
        with open('trips.csv', 'rU') as f:  
            reader = csv.reader(f)
            for row in reader:
                trips.append(row)
    return trips
            

def check_last_trip():
    if os.path.isfile('last_trip.txt') == False:
        lastrip_id = 0
    else:
        text_file = open('last_trip.txt', 'r')
        lastrip_id = text_file.readline()

    return int(lastrip_id)


def record_last_trip(trip_id):
    # write the trip that we just processed
    f = open('last_trip.txt', 'w')
    f.write(str(trip_id))


def find_od_zones(lat, lon):
    # this function will define the origin zone and destination zone
    # https://network-production.herokuapp.com/api/v1.5/census?city=elpaso&lat=31.7851667&lon=-106.4707744
    lat_lon = 'lat={}&lon={}'.format(lat, lon)
    api = pip_api  + lat_lon
    try:
        r = requests.get(api)
        zone = r.json()['zone_id']
    except:
        # print ('zone outside census', lat, lon)
        zone = 'NA'

    return zone


def write_to_db(trip):
    # this function writes the new data on the DB
    sql = """INSERT INTO od_geocoding (tripid, uid, random_id, routedistance, tripdistance, estimate_tt, actual_tt, local_start_time, local_end_time, local_hh, 
            local_weekday, local_ww, local_mm, localyyyy, startcity, endcity, startlat, startlon, endlat, endlon, startzone, endzone)
			VALUES (%s, %s, %s, %s, %s, %s, %s, '%s', '%s', %s, '%s', %s, %s, %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', 
            '%s')"""  % (trip[0], trip[1], trip[2], trip[3], trip[4], trip[5], trip[6], trip[7], trip[8], trip[9], trip[10], trip[11], trip[12], trip[13], trip[14], trip[15], trip[16], trip[17], trip[18], trip[19], trip[20], trip[21])
    #my_db_cursor.executemany(sql, data)
    my_db_cursor.execute(sql)
    my_db_cnxn.commit()


def write_to_csv(row):
    fields = ['tripid', 'uid', 'random_id', 'routedistance', 'tripdistance', 'estimate_tt', 'actual_tt', 'local_start_time', 'local_end_time', 'local_hh', 
            'local_weekday', 'local_ww', 'local_mm', 'localyyyy', 'startcity', 'endcity', 'startlat', 'startlon', 'endlat', 'endlon', 'startzone', 'endzone']
    
    if os.path.isfile('final_output.csv') == False:
        with open("final_output.csv", "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(fields)
            writer.writerow(row)
    else:
        with open(r'final_output.csv', 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(row)
    #f.close()


def main(trip):
    # this is the main function that will loop over all the trips
    last = check_last_trip() 
    if int(trip[0]) > last: # this makes sure we pick up from the last trip we checked
        start_zone = find_od_zones(trip[16], trip[17]) 
        end_zone = find_od_zones(trip[18], trip[19])
        zones = [start_zone, end_zone]
        row = trip + zones
        write_to_csv(row)
        write_to_db(row)
        record_last_trip(trip[0])
    else:
        pass

    return 'done'


if __name__ == '__main__':
    print('starting OD Geocoding')
    start = time.time()
    trips = get_trips()
    get_trips_end = time.time()
    print('getting the trips took', get_trips_end - start)
    pool = Pool(8)
    pool.map(main, trips)
    #done = main(trips)
    end = time.time()
    print('main zone write loop took', end - start)
    print ('done')
