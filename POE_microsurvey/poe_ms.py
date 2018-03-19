# this script will send microsurveys to daily POE users to check which bridge did they use to cross on their trip.Exception
import pyodbc
import config
from datetime import datetime, timedelta
import json
import requests
import mysql.connector as mariadb
from neo4j.v1 import GraphDatabase, basic_auth


rpt_db_cnxn = pyodbc.connect(config.RPTDB_CNXN)
rpt_db_cursor = rpt_db_cnxn.cursor()
ms_db_cnxn = mariadb.connect(host=config.MS_DB_URL, user=config.MS_DB_USER, passwd=config.MS_DB_PASSWORD, db=config.MS_DB_DB, port=3306)
ms_db_cursor =  ms_db_cnxn.cursor()
neo_driver = GraphDatabase.driver(config.NEO_URL, auth=basic_auth(config.NEO_USER, config.NEO_PASSWORD))
neo_session = neo_driver.session()
poe_db_cnxn = mariadb.connect(host=config.POE_DB_URL, user=config.POE_DB_USER, passwd=config.POE_DB_PASSWORD, db=config.POE_DB_DB, port=3300)
poe_db_cursor = poe_db_cnxn.cursor()

def get_daily_poe_trips_from_db():
    # query daily list of trips
	query = """ SELECT TOP 5 TripSummaryID, MetropianID, StartAddress, endaddress, localstarttime
				FROM  rpttripsummary
				 WHERE LocalYYYYMMDD = '20180301' and TripMetroCity = 'ElPaso' and StartCity = 'Juarez' 
				and EndCity = 'ElPaso' and DistanceFromTrajectory > 1 """
	rpt_db_cursor.execute(query)
	data = rpt_db_cursor.fetchall()
	user_trip_data = {}
	# For now we are only doing one user per trip due to limitations in microsurvey and repartdatabase
	for row in data:
		# user trip data [metropian_id] = tripsummaryid, startaddress, endaddress, localstarttime 
		#user_trip_data[row[1]] = row[0], row[2], row[3], row[4]
	    user_trip_data[335] = 1, 'poopstart', 'poopend', '2018-03-18 12:24:15' # testing
	
	return user_trip_data


def get_daily_poe_trips_from_api():
	# queries conans api to get all of the poe trips for the day
	user_trip_data = {}
	yesterday = datetime.now() - timedelta(days=1)
	# http://production.metropia.com/api/v1/poe_crossing/drive?date=2018-03-15
	url = config.POE_API + yesterday.strftime('%Y-%m-%d')
	data = requests.get(url=url).json()
	trips_l = data['data']['crossing_trips']
	for trip in trips_l:
		if trip['direction'] == 0:
			uid, tripid = trip['user_id'], trip['trip_id']
			poe, duration = trip['poe'], trip['trip_duration']
			crossing_time = trip["crossing_datetime"]
			if duration is None or tripid is None:
				duration, tripid = 'nop', 'nop'
			user_trip_data[uid]= tripid, poe, duration, crossing_time
	return user_trip_data


def suffix(d):
    return 'th' if 11<=d<=13 else {1:'st',2:'nd',3:'rd'}.get(d%10, 'th')


def user_on_neo4j(userid):
	neo4j_query = 'MATCH (n:metropia_user {userid : %d}) return n' %(userid)
	results = neo_session.run(neo4j_query)
	if results.peek() == None:
		neo4j_query = 'Create (n:metropia_user {userid: %d})' %(userid)
		neo_session.run(neo4j_query)
	return True


def user_on_msdb(userid):
	sql_check = "SELECT MetropianID FROM microsurvey.User WHERE MetropianID = %s" %(userid)
	ms_db_cursor.execute(sql_check)
	results = ms_db_cursor.fetchall()
	if len(results) > 0:
		return True
	else:
		sql_create = "INSERT INTO microsurvey.User (MetropianID) VALUES (%s)" %(userid)
		ms_db_cursor.execute(sql_create)
		ms_db_cnxn.commit()
		return True
	return False


def check_user_on_db(userid):
	if user_on_neo4j(userid) and user_on_msdb(userid):
		return True
	else:
		False
	

def get_question_title(questionid):
	sql = 'SELECT QuestionItem FROM Question WHERE idQuestion=%s' %(questionid)
	ms_db_cursor.execute(sql)
	results = ms_db_cursor.fetchall()[0][0]
	return results


def get_question_answers(questionid):
	sql = 'SELECT b.idanswer, b.AnswerItem FROM Q_A_Relation a, Answer b WHERE a.idquestion=%s and a.idanswer=b.idanswer' %(questionid)
	ms_db_cursor.execute(sql)
	results = ms_db_cursor.fetchall()
	shoot =[]
	for answer in results:
		shoot.append([answer[0], str(answer[1])])
	return shoot


def get_question_points(questionid):
	sql = 'SELECT b.Point_amount FROM P_Q_Relation a, Points b WHERE a.Question_idQuestion=%s and a.Points_idPoints =b.idpoints' %(questionid)
	ms_db_cursor.execute(sql)
	results = ms_db_cursor.fetchall()
	return results[0][0]


def prepare_payload(userid, q_id, q_title, answers, points, title):
	response_api = config.MS_RESPONSE_URL + str(userid)
	payload = { 
		"user_id": userid,
		"data": {
			"message": {
				"type": "microsurvey",
				"user_id": userid,
				"message": "Answer this quick question to earn reward points!",
				"question": {
					"id": q_id,
					"type": 1, 					
					"display": 1,
					"mpoint": points,
					"data": {
						"title": title,
						"body": q_title,
						"answer": answers
					},
					"response_api": response_api
				}
			}
		}
	}
	return json.dumps(payload)


def send_to_PN(payload):
	url = config.MS_PUSH_PD_URL
	response = requests.post(url, data=payload, headers={"Content-Type": "application/json"})
	response = json.loads(response.text)
	
	return response


def morning_or_afternoon(crossing_date):
	parts_of_the_day = {'in the early morning on':[1,2,3,4,5], 'in the morning on':[6,7,8,9,10,11], 'around noon on':[12], 
						'in the afternoon on':[13,14,15,16,17], 'in the evening on':[18,19,20], 'in the night of':[21,22,23], 'around midnight on':[0]}
	# am_pm = crossing_date.strftime("%p")
	# hour = "{:d}:{:02d} {}".format(crossing_date.hour, crossing_date.minute, am_pm.lower())
	date = '{dt:%A}, {dt:%B} {dt.day}{s}'.format(dt=crossing_date, s=suffix(crossing_date.day))
	hour = int(crossing_date.hour)
	for k, v in parts_of_the_day.iteritems():
		if hour in v:
			part_of_day = k
	partofday_date = ' {} {}?'.format(part_of_day, date)

	return partofday_date


def send_ms_main(user_trip_dict):
	q_id = config.QUESTION_ID
	question_title = str(get_question_title(q_id))
	answers = get_question_answers(q_id)
	points = get_question_points(q_id)
	title = "Answer to earn %s points!" % (points)
    # for uid, value in user_trip_dict.items():          #for python 3
	for uid, value in user_trip_dict.iteritems():
		# print (check_user_on_db(uid)) 
		if check_user_on_db(uid) == True:
			crossing_date = datetime.strptime(value[3], '%Y-%m-%d %H:%M:%S')
			hour_date = morning_or_afternoon(crossing_date)
			q_title = ''
			q_title = question_title + hour_date
			# print (q_title)
			payload = prepare_payload(uid, q_id, q_title, answers, points, title)
			response = send_to_PN(payload)
			# response = {"status": "success", "data": {"msg": "", "type": "push_notification", "user_id": 335}}
			r_status = response["status"]
			# print r_status
			sql = """INSERT INTO poe_microsurvey (uid, tripid, poe, trip_duration, trip_date, question_id, question, points, status, add_date)
			VALUES (%s, '%s', '%s', '%s', '%s', %s, '%s', %s, '%s', '%s')""" % (uid, value[0], value[1], value[2], crossing_date, q_id, q_title, points, r_status, datetime.now())
			poe_db_cursor.execute(sql)
		else:
			pass
	poe_db_cnxn.commit()

	return 'done'

if __name__ == '__main__':
	print('starting POE microsurvey')
	# daily_user_trips = get_daily_poe_trips_from_api()
	daily_user_trips = get_daily_poe_trips_from_db()
	do = send_ms_main(daily_user_trips)
	print (do)

    