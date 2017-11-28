import urllib2
import json
import numpy as np

def get_forecast():
	f = urllib2.urlopen('http://api.wunderground.com/api/40c1e03239029f36/forecast/q/RI/Providence.json')
	json_string = f.read()
	parsed_json = json.loads(json_string)
	
	windspeeds = []
	
	for i in parsed_json['forecast']['simpleforecast']['forecastday']:
	  wind_mph = i['avewind']['mph']
	  print wind_mph
	  snow_allday = i['snow_allday']['in']
	  print snow_allday
	  high_temp = i['high']['fahrenheit']
	  print high_temp
	  low_temp = i['low']['fahrenheit']
	  print low_temp
	  
	  windspeeds.append(float(wind_mph))
	
	f.close()
	
	return np.array(windspeeds)
