import sys
import os
import json
import pandas as pd

# Get the number of elements to retreive from the command line
number_of_airport_to_retreive = int(sys.argv[1])

path_to_json = ""
with open('./config.json') as json_file:
    json_data = json.load(json_file)
    path_to_json = json_data["data"]

file = open(path_to_json, 'r')
count = 0
for line in file:
    if count < number_of_airport_to_retreive:
        print(line.strip())
        count = count + 1
    else:
        file.close()
        break
sys.stdout.flush()
