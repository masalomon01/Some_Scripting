'''
	This script converts
	Line segments from 
	[long lat][long lat].........[long lat]
	to
	LINESTRING (lat lon, lat lon, lat lon)
'''
FILENAME = 'transtar_travel_time_segments_shrp2_map.csv'

import csv

def get_input_data(input_file):
    input_data = []
    with open(input_file, 'rb') as f:  # getting input data
        reader = csv.reader(f)
        input_data = list(reader)
    return input_data


def convert_2wkt(data):
	wkt_lol = [data[0]]
	for each in data[1:]:
		str_wkt = ""
		str_of_points = str(each[6:])  # poorly formated list because of the commas between the points
		rmv = "',"
		for char in rmv:
			str_of_points = str_of_points.replace(char,"")
		str_of_points = str_of_points[1:-1].replace(']',"],")
		l_of_points = str_of_points[:-1].split(',') # now we have a formated list
		for point in l_of_points:
			lat = point.rsplit(' ', 1)[1]
			lon = point.rsplit(' ', 1)[0]
			str_point = lon[1:] + " "  + lat[:-1] + ", "
			str_wkt+=str_point
		str_wkt = "LINESTRING (" + str_wkt[:-2] + ")"
		lwkt = each[0:6]
		lwkt.append(str_wkt)
		wkt_lol.append(lwkt)
	return wkt_lol


if __name__ == '__main__':
	in_data = get_input_data(FILENAME)
	conversion = convert_2wkt(in_data)
	with open('transtar_WKT.csv', 'wb') as f:
		writer = csv.writer(f)
		writer.writerows(conversion)
