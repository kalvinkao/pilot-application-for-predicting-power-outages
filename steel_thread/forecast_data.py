import urllib2
import json
f = urllib2.urlopen('http://api.wunderground.com/api/40c1e03239029f36/forecast/q/RI/Providence.json')
json_string = f.read()
parsed_json = json.loads(json_string)
for i in parsed_json['forecast']['simpleforecast']['forecastday']:
  wind_mph = i['avewind']['mph']
  print wind_mph
  snow_allday = i['snow_allday']['in']
  print snow_allday
  high_temp = i['high']['fahrenheit']
  print high_temp
  low_temp = i['low']['fahrenheit']
  print low_temp
f.close()
