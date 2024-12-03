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
# META     },
# META     "environment": {
# META       "environmentId": "baaac75d-e36a-46db-85ca-5c4b7827a9db",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Bronze Layer Notebook: Earthquake Data Ingestion
# 
# This notebook extracts raw earthquake data from an external API and stores it in the Lakehouse's Bronze Layer. It is designed as the initial step in the data pipeline, providing raw data for subsequent transformations in the Silver and Cold Layers.
# 
# ---
# 
# ## Objectives
# 
# 1. **Data Extraction**:
#    - Fetch raw earthquake data from the United States Geological Survey (USGS) API for a specified date range (two days prior to one day prior to execution).
# 
# 2. **Data Validation**:
#    - Ensure the API request is successful and that the data contains valid earthquake event records.
# 
# 3. **Data Storage**:
#    - Save the fetched data in JSON format to the Lakehouse's Bronze Layer for use in downstream processing.
# 
# 4. **Error Handling**:
#    - Gracefully handle errors related to API failures, missing data, or file write operations.
# 
# ---
# 
# ## Workflow
# 
# 1. **Define Date Range**:
#    - Calculate the date range for the API request dynamically, ensuring the notebook processes data for the two days before execution.
# 
# 2. **Fetch Data from API**:
#    - Send a request to the USGS earthquake event API using the defined date range.
#    - Parse the API response to extract earthquake event data.
# 
# 3. **Validate API Response**:
#    - Check the API response status to confirm successful data retrieval.
#    - Validate that the dataset contains earthquake event records for the specified date range.
# 
# 4. **Save Data to Bronze Layer**:
#    - Write the validated data in JSON format to a specified file path in the Lakehouse's Bronze Layer.
# 
# 5. **Error Logging**:
#    - Log errors for any failed API requests or data write operations for easier debugging and monitoring.
# 
# ---
# 
# ## Outputs
# 
# - **Raw Data File**:
#   - A JSON file containing earthquake event data, saved to the Lakehouse Bronze Layer.
#   - File naming convention: `<start_date>_earthquake_data.json`.
# 
# ---
# 
# ## Notes
# 
# - **Dynamic Date Handling**:
#   - The notebook dynamically calculates the date range for the API request, ensuring fresh data for each execution.
# 
# - **Error Management**:
#   - Includes robust error handling for API failures, empty datasets, and file write issues, with clear logging for troubleshooting.
# 
# - **Scalability**:
#   - Designed to process earthquake data of varying sizes efficiently by leveraging JSON file storage for raw data.
# 
# - **Data Format**:
#   - The output JSON file retains the raw structure of the API response for maximum flexibility in downstream processing.


# CELL ********************

import requests
import json
import os
from datetime import date, timedelta

start_date = date.today()-timedelta(2)
end_date= date.today()-timedelta(1)

def fetch_earthquake_data(url, file_path):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            data = data['features']

            if not data:
                print("No data available for the given date range.")
                return False

            # Save data to the lakehouse
            with open(file_path, 'w') as file:
                json.dump(data, file, indent=4)
            
            print(f"Data successfully saved to {file_path}")
            return True
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            return False
    except Exception as e:
        print(f"Error while fetching data: {e}")
        return False

url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}&endtime={end_date}"
file_path = f'/lakehouse/default/Files/{start_date}_earthquake_data.json'

if fetch_earthquake_data(url, file_path):
    print("Data extraction completed successfully.")
else:
    raise Exception("Data extraction failed. Check logs for details.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
