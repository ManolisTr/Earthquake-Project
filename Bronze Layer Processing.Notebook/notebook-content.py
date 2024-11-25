# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f606ae08-eca5-4e72-ab37-a570aa76af35",
# META       "default_lakehouse_name": "earthquakes_lakehouse",
# META       "default_lakehouse_workspace_id": "8a962f84-b28e-4d61-b005-97d877431b8d"
# META     }
# META   }
# META }

# CELL ********************

from datetime import date, timedelta

start_date = date.today()- timedelta(7)
end_date= date.today()-timedelta(1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json

url=f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}&endtime={end_date}"

response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    data = data['features']

    file_path = f'/lakehouse/default/Files/{start_date}_earthquake_data.json'

    with open(file_path, 'w') as file:
        json.dump(data,file,indent=4)
    
    print(f'Data Successfully saved to {file_path}')
else:
    print("Failed to fetch data. Status code:",response.status_code)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("multiline", "true").json("Files/2024-11-18_earthquake_data.json")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
