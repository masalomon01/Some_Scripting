__author__ = 'Will'

import arcpy
import math
from math import log

def Bearing(slat,slong,elat,elong):
	startLat = math.radians(slat)
	startLong = math.radians(slong)
	endLat = math.radians(elat)
	endLong = math.radians(elong)

	dLong = endLong - startLong

	temp = math.tan(endLat/2.0+math.pi/4.0)/math.tan(startLat/2.0+math.pi/4.0)
	temp = abs(temp)   # I added this because it was throwing an error with negative numbers, this is dangerous because I don't know what this function is doing so keep an eye
	dPhi = math.log(temp)
	if abs(dLong) > math.pi:
		 if dLong > 0.0:
			 dLong = -(2.0 * math.pi - dLong)
		 else:
			 dLong = (2.0 * math.pi + dLong)

	bearing = (math.degrees(math.atan2(dLong, dPhi)) + 360.0) % 360.0

	return bearing

def CalDirect(bearing):
    if (bearing >= 0 and bearing <= 22.5)or(bearing > 337.5 and bearing <= 360):
        return "N"
    elif bearing > 22.5 and bearing <= 67.5:
        return "NE"
    elif bearing > 67.5 and bearing <= 112.5:
        return "E"
    elif bearing > 112.5 and bearing <= 157.5:
        return "SE"
    elif bearing > 157.5 and bearing <=202.5:
        return "S"
    elif bearing > 202.5 and bearing <=247.5:
        return "SW"
    elif bearing > 247.5 and bearing <=302.5:
        return "W"
    elif bearing > 302.5 and bearing <=337.5:
        return "NW"

#this should be where the original shapefiles are located
arcpy.env.workspace = "C:/Users/Mario/Documents/GitHub/Some_Scripting/Houston Network Matching"
#this is where you want to store results
out_folder_path = "C:/Users/Mario/Documents/GitHub/Some_Scripting/Houston Network Matching"
in_shapefiles = ["Houston_Barebones_1KM.shp","Heng_net_UTMZ15.shp"]
ptv = 'Heng_net_UTMZ15_shp'
osm = 'Houston_Barebones_1KM_shp'
print ("workspace set up")

#Create a new file geodatabase to store the data

out_name = "data2.gdb"
arcpy.CreateFileGDB_management(out_folder_path, out_name, "CURRENT")

print ("file gdb created")

#import shapefiles into the file geodatabase
out_location = out_folder_path + "/data2.gdb"
arcpy.FeatureClassToGeodatabase_conversion(in_shapefiles,out_location)

print ("shapefiles imported into file gdb")

#set workspace to the newly created file geodatabase
arcpy.env.workspace = out_location

print ("workspace set to new file gdb")

#Buffer the PTV data
arcpy.Buffer_analysis(ptv,"PTV_buffer","100 Meters","FULL","ROUND","ALL")

print ("ptv initial buffer created")

#clip the OSM data to extract only links within 20 meters of a PTV link
arcpy.Clip_analysis(osm,"PTV_buffer","OSM_clipped")

print ("OSM clipped")

#update current feature class 
osm = ("OSM_clipped")

# split lines at vertices
arcpy.SplitLine_management(osm, "OSM_split")
arcpy.SplitLine_management(ptv, "PTV_split")

print ("OSM and PTV split")

osm = "OSM_split"
ptv = "PTV_split"

arcpy.AddField_management(ptv,"PTV_bear", "SHORT")
arcpy.AddField_management(ptv,"PTV_direct", "TEXT")

cursor = arcpy.da.UpdateCursor(ptv,['SHAPE@','PTV_bear','PTV_direct'])

for row in cursor:
	#x is long
	#y is lat
	startx = row[0].firstPoint.X
	starty = row[0].firstPoint.Y

	endx = row[0].lastPoint.X
	endy = row[0].lastPoint.Y

	comma = ","

	bear = Bearing(starty,startx,endy,endx)
	row[1] = bear
	row[2] = CalDirect(bear)
	cursor.updateRow(row)

arcpy.AddField_management(osm,"OSM_bear", "SHORT")
arcpy.AddField_management(osm,"OSM_direct", "TEXT")

cursor = arcpy.da.UpdateCursor(osm,['SHAPE@','OSM_bear','OSM_direct'])

for row in cursor:
	#x is long
	#y is lat
	startx = row[0].firstPoint.X
	starty = row[0].firstPoint.Y

	endx = row[0].lastPoint.X
	endy = row[0].lastPoint.Y

	comma = ","

	bear = Bearing(starty,startx,endy,endx)
	row[1] = bear
	row[2] = CalDirect(bear)
	cursor.updateRow(row)


