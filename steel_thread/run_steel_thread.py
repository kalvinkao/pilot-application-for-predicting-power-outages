from pyspark.sql import SQLContext
from pyspark.sql.types import *
import steel_thread
from pyspark import SparkContext

sc = SparkContext()
sqlContext = SQLContext(sc)

outageData=sc.textFile("file:///home/w205/steel_thread/outage_history.csv")
weatherData=sc.textFile("file:///home/w205/steel_thread/weather_history.csv")

riOutages = outageData.filter(lambda x: "Rhode Island" in x)
riOutageRecords = riOutages.map(lambda r : r.split(","))
weatherRecords = weatherData.map(lambda r : r.split(","))

RI_Outages = riOutageRecords.map(lambda p: (p[2],p[4],p[7],p[10]))
#delimiters are all fucked up for the outage data
RI_Weather = weatherRecords.map(lambda p: (p[5],p[10],p[16], p[17], p[18], p[19]))

outageSchemaString = 'DATETIME DURATION AREA NUMCUSTOMERS'
weatherSchemaString = 'DATETIME DRYBULBTEMP HUMIDITY WINDSPEED WINDDIRECTION WINDGUSTSPEED'

outageFields = [StructField(field_name, StringType(), True) for field_name in outageSchemaString.split()]
weatherFields = [StructField(field_name, StringType(), True) for field_name in weatherSchemaString.split()]

outageSchema = StructType(outageFields)
weatherSchema = StructType(weatherFields)

schemaOutageData = sqlContext.createDataFrame(RI_Outages, outageSchema)
schemaWeatherData = sqlContext.createDataFrame(RI_Weather, weatherSchema)

schemaOutageData.registerTempTable('RI_Outages')
schemaWeatherData.registerTempTable('RI_Weather')

results = sqlContext.sql('SELECT * FROM RI_Weather LIMIT 10')
results.show()

print(steel_thread.random_prediction())