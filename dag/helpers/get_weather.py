import requests
import config as C 
import json
from datetime import datetime 
import os

def get_weather():
	"""
	Query openweathermap.com's API and to get the weather for
	Brooklyn, NY and then dump the json to the ../data/ directory
	with the file name "<today's date>.json"
	"""

	paramaters = {'q': 'Brooklyn, USA', 'appid':C.API_KEY}

	result     = requests.get("http://api.openweathermap.org/data/2.5/weather?", paramaters)

	if result.status_code == 200 :

		json_data = result.json()
		file_name  = str(datetime.now().date()) + '.json'
		tot_name   = os.path.join(os.path.dirname(__file__), '../data', file_name)

		with open(tot_name, 'w') as outputfile:
			json.dump(json_data, outputfile)
	else :
		print "Error In API call."


if __name__ == "__main__":
	get_weather()
