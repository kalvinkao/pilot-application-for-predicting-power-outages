import urllib2
import json
import numpy as np
from datetime import datetime

def get_forecast():
        f = urllib2.urlopen('http://api.wunderground.com/api/40c1e03239029f36/forecast/q/RI/Providence.json')
        json_string = f.read()
        parsed_json = json.loads(json_string)

        windspeeds = []

        for i in parsed_json['forecast']['simpleforecast']['forecastday']:
          wind_mph = i['avewind']['mph']
         # print wind_mph
          snow_allday = i['snow_allday']['in']
          # print snow_allday
          high_temp = i['high']['fahrenheit']
          # print high_temp
          low_temp = i['low']['fahrenheit']
         # print low_temp

          windspeeds.append(float(wind_mph))

        f.close()

        return np.array(windspeeds)

def get_dates():
  f = urllib2.urlopen('http://api.wunderground.com/api/40c1e03239029f36/forecast/q/RI/Providence.json')
  json_string = f.read()
  parsed_json = json.loads(json_string)
  dates = []
  for i in parsed_json['forecast']['simpleforecast']['forecastday']:
    day = i['date']['day']
    month = i['date']['monthname_short']
    year = i['date']['year']
    hour = i['date']['hour']
    min = i['date']['min']
    ampm = i['date']['ampm']
    date_time = str(month) + ' ' + str(day) + ' ' + str(year) + '  ' + str(hour)+':'+str(min)+str(ampm)
    dates.append(date_time)
  print dates
  formatted_dates = []

  for d in dates:
    date = datetime.strptime(d,'%b %d %Y %H:%M%p')
    print type(date)
    print date
    formatted_dates.append(date)

  f.close()
  return np.array(formatted_dates)
