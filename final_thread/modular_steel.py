from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.types import *
#import steel_thread
import ml_processing
from pyspark import SparkContext
#import forecast_data_v3
import forecast_data_v4
import numpy as np
import pandas as pd

sc = SparkContext()
hive_context = HiveContext(sc)
sqlContext = SQLContext(sc)

outageData=sc.textFile("file:///home/w205/w205_final_project/final_thread/outage_history.csv")

states = ["Rhode Island", "Massachusetts"]
# Get state outage and weather data
def get_state_data():
  for s in states:
    print(s)
    outage = outageData.filter(lambda x: s in x)
    stateoutage = outage.map(lambda r: r.split(","))
    if len(s) > 1:
      s = s.replace(" ", "_")
    else:
      continue
    filename = "file:///home/w205/w205_final_project/final_thread/weather_history_" + s + ".csv"
    print(filename)
    weatherData = sc.textFile(filename)
    weatherRecords = weatherData.map(lambda r: r.split(","))
    Outages = stateoutage.map(lambda p: (p[2], p[4], p[5], p[8], p[12]))
    State_Weather = weatherRecords.map(lambda p: (p[5], p[6], p[26], p[27], p[28], p[30], p[37], p[38], p[39], p[40], p[41], p[42], p[43], p[44], p[46]))
    outageSchemaString = 'DATETIME HR MIN AREA NUMCUSTOMERS'  # If the above gets updated, this would too (of course)
    weatherSchemaString = 'DTS ReportType maxTemp minTemp aveTemp aveHumidity WeatherCodes Precip Snowfall SnowDepth aveStationPressure aveSeaLevelPressure aveWindSpeed maxWindSpeed SustainedWindSpeed'
    outageFields = [StructField(field_name, StringType(), True) for field_name in outageSchemaString.split()]
    weatherFields = [StructField(field_name, StringType(), True) for field_name in weatherSchemaString.split()]
    outageSchema = StructType(outageFields)
    weatherSchema = StructType(weatherFields)
    schemaOutageData = sqlContext.createDataFrame(Outages, outageSchema)
    schemaWeatherData = sqlContext.createDataFrame(State_Weather, weatherSchema)
    schemaOutageData.registerTempTable('State_Outages')
    show_outages = sqlContext.sql('SELECT * FROM State_Outages')
    show_outages.show()
    schemaWeatherData.registerTempTable('State_Weather')
    show_weather = sqlContext.sql('SELECT * FROM State_Weather')
    show_weather.show()
    result_weatherOutage = sqlContext.sql('SELECT to_date(w.DTS) as DT ,w.maxTemp ,w.minTemp ,w.aveTemp ,w.aveHumidity ,w.WeatherCodes ,w.Precip ,w.Snowfall ,w.SnowDepth ,w.aveStationPressure ,w.aveSeaLevelPressure ,w.aveWindSpeed ,w.maxWindSpeed, w.SustainedWindSpeed ,case when o.DATETIME is null then 0 else 1 end as OutageIND  FROM RI_Weather w left outer join RI_Outages o on to_date(w.DTS) = to_date(concat(substr(DATETIME,7,4),"-",substr(DATETIME,1,2),"-",substr(DATETIME,4,2)))   WHERE w.ReportType="SOD" and year(to_date(w.DTS))=2016 and month(to_date(w.DTS))=2  ORDER BY DT  LIMIT 100')
    result_weatherOutage.show()
    data = result_weatherOutage.select('aveWindSpeed', 'maxTemp', 'minTemp', 'aveHumidity').collect()
    return data

print(get_state_data())
