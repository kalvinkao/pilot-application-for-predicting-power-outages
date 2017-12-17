from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.types import *
import ml_processing  #import steel_thread
from pyspark import SparkContext
import forecast_data_v4  #import forecast_data_v3
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
    # Do we need to blow away the pointers/tables/other vars first?
    
    # prepare the outage data for the state
    outage = outageData.filter(lambda x: s in x)
    stateoutage = outage.map(lambda r: r.split(","))
    Outages = stateoutage.map(lambda p: (p[2], p[4], p[5], p[8], p[12]))   # I could not figure out how to properly parse this...
    outageSchemaString = 'DATETIME HR MIN AREA NUMCUSTOMERS'  # If the above gets updated, this would too (of course)
    outageFields = [StructField(field_name, StringType(), True) for field_name in outageSchemaString.split()]
    outageSchema = StructType(outageFields)
    schemaOutageData = sqlContext.createDataFrame(Outages, outageSchema)
    schemaOutageData.registerTempTable('State_Outages')
    show_outages = sqlContext.sql('SELECT * FROM State_Outages')
    #show_outages.show()
    
    # prepare weather history data
    if len(s) > 1:
      s_ = s.replace(" ", "_")
    else:
      continue
    # Assumes weather history files will be in directories by state
    filename = "file:///home/w205/w205_final_project/final_thread/weather_history_" + s_ + ".csv"
    print(filename)
    weatherData = sc.textFile(filename)
    weatherRecords = weatherData.map(lambda r: r.split(","))
    State_Weather = weatherRecords.map(lambda p: (p[5], p[6], p[26], p[27], p[28], p[30], p[37], p[38], p[39], p[40], p[41], p[42], p[43], p[44], p[46]))
    weatherSchemaString = 'DTS ReportType maxTemp minTemp aveTemp aveHumidity WeatherCodes Precip Snowfall SnowDepth aveStationPressure aveSeaLevelPressure aveWindSpeed maxWindSpeed SustainedWindSpeed'
    weatherFields = [StructField(field_name, StringType(), True) for field_name in weatherSchemaString.split()]
    weatherSchema = StructType(weatherFields)
    schemaWeatherData = sqlContext.createDataFrame(State_Weather, weatherSchema)
    schemaWeatherData.registerTempTable('State_Weather')
    show_weather = sqlContext.sql('SELECT * FROM State_Weather')
    show_weather.show()
    
    # combine outage and weather history datasets
    result_weatherOutage = sqlContext.sql('SELECT to_date(w.DTS) as DT ,w.maxTemp ,w.minTemp ,w.aveTemp ,w.aveHumidity ,w.WeatherCodes ,w.Precip ,w.Snowfall ,w.SnowDepth ,w.aveStationPressure ,w.aveSeaLevelPressure ,w.aveWindSpeed ,w.maxWindSpeed, w.SustainedWindSpeed ,case when o.DATETIME is null then 0 else 1 end as OutageIND  FROM State_Weather w left outer join Outages o on to_date(w.DTS) = to_date(concat(substr(DATETIME,7,4),"-",substr(DATETIME,1,2),"-",substr(DATETIME,4,2)))   WHERE w.ReportType="SOD" and year(to_date(w.DTS))=2016 and month(to_date(w.DTS))=2  ORDER BY DT  LIMIT 100')
    #result_weatherOutage.show()
    
    # process combined dataset, first training data then training labels.  Maybe combine to one loop?
    feature_data = result_weatherOutage.select('aveWindSpeed', 'maxTemp', 'minTemp', 'aveHumidity').collect()
    train_data = []
    train_labels = []
    for a in feature_data:
        aveWindSpeed = float('nan')
        maxTemp = float('nan')
        minTemp = float('nan')
        #aveHumidity = float('nan')
        if a.aveWindSpeed:
            aveWindSpeed = float(a.aveWindSpeed)
        if a.maxTemp:
            maxTemp = float(a.maxTemp)
        if a.minTemp:
            minTemp = float(a.minTemp)
        if a.aveHumidity:
            aveHumidity = float(a.aveHumidity)
        train_data.append([aveWindSpeed, maxTemp, minTemp])

    label_data = result_weatherOutage.select('OutageIND').collect()
    for a in label_data:
        if np.isnan(a.OutageIND):
            train_labels.append(float('nan'))
        else:
            train_labels.append(float(a.OutageIND))

    #train_data_temp = np.array(train_data).reshape(-1,1)
    train_data_temp = np.array(train_data)
    train_labels_temp = np.array(train_labels).reshape(-1,1)

    # Does this properly keep the same records?
    train_labels = train_labels_temp[~np.isnan(train_data_temp).any(1)]
    train_data = train_data_temp[~np.isnan(train_data_temp).any(1)]
    test_data = forecast_data_v4.get_forecast()

    result_probabilities, result_predictions = ml_processing.lr_prediction(train_data, train_labels, test_data)
    outage_probabilities = result_probabilities[:,1]
    prediction_results = result_predictions


    dates = forecast_data_v4.get_dates()
    
    
    return s, outage_probabilities, prediction_results, dates

print(get_state_data())
