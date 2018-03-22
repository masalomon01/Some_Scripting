import time
import arcpy

arcpy.env.workspace= "C:/Users/Mario/Documents/GitHub/Some_Scripting/Houston Network Matching/data2.gdb"
fc= "OSM_toPoints"
fc2= "OSM_split"

ID= "gid"
Dist= "N_dist"
DistDict= {}

cursor= arcpy.da.SearchCursor(fc,[ID,Dist])

for row in cursor:
    key= row[0]
    dist= row[1]
    DistDict.setdefault(key,[]).append(dist)

del row
del cursor

out=open('C:/Users/Mario/Documents/GitHub/Some_Scripting/Houston Network Matching/Buffdist.txt','w')

for key in DistDict:
    out.write(str(key))
    out.write('\t')
    out.write(str(DistDict[key]))
    out.write('\n')

out.close()
print ("distance extraction complete!")
print (time.ctime())

starttime=time.clock()



cursor= arcpy.da.UpdateCursor(fc2,[ID,Dist])

for row in cursor:
    if row[0] in DistDict:
        row[1]=max(DistDict[row[0]])
        #print "added"
        cursor.updateRow(row)

endtime= time.clock()

print (time.ctime())
update_time =  "update finished in %d seconds" % (endtime-starttime)
print (update_time)

del row
del cursor



