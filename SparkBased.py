from pyspark import SparkContext
import time
from pathlib import Path
import shutil
from pyspark import SQLContext

def mapper(data):
	date = data[15:23]
	temp_air=int(data[87:92])
	wind_speed = int(data[65:69])

	return (date[0:4],temp_air,wind_speed);

def mapper1(data):
	date = data[15:23]
	temp_air=data[87:92]
	wind_speed = int(data[65:69])

	return (date[0:4],(wind_speed));




#initializing the context
sc = SparkContext(appName="Climate Analysis")
sc.setLogLevel("ERROR")
#reading the whole directory
start_time = time.time()
lines = sc.textFile("/home/DATA/NOAA_weather/1980/*.gz")
#lines = sc.textFile("./*.gz")

lines1=lines.map(mapper)

output1_name = "./output1/"
output1_folder = Path(output1_name)
if output1_folder.is_dir():
    shutil.rmtree(output1_name)

min_temp = lines1.map(lambda x:(x[0],x[1]))\
			   .filter(lambda x : x[1]!=9999)\
			   .reduceByKey(lambda x,y:min(x,y))


max_temp = lines1.map(lambda x:(x[0],x[1]))\
			 .filter(lambda x : x[1]!=9999)\
			 .reduceByKey(lambda x,y : max(x,y))


max_windspeed=lines1.map(lambda x:(x[0],x[2]))\
			  .filter(lambda x : x[2]!=9999)\
			  .reduceByKey(lambda x,y : max(x,y))


def toCSVLine(data):
  return ','.join(str(d) for d in data)




output = sc.union([min_temp,max_temp,max_windspeed]).reduceByKey(lambda x,y :(x,y))



output.collect()
print("--- %s seconds ---" % (time.time() - start_time))
lines = output.map(toCSVLine)
lines.saveAsTextFile(path=output1_name)



sc.stop()

