# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "baaac75d-e36a-46db-85ca-5c4b7827a9db",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # URL Validation Notebook: Pre-Ingestion Check
# 
# This notebook validates the accessibility of the earthquake data API before proceeding with the data ingestion pipeline. It ensures that the pipeline only continues if the API is accessible, preventing unnecessary failures downstream.
# 
# ---
# 
# ## Objectives
# 
# 1. **Validate API Accessibility**:
#    - Perform a `HEAD` request to the earthquake data API to check its availability and responsiveness.
# 
# 2. **Pipeline Readiness**:
#    - Confirm that the API is accessible before initiating data extraction.
#    - Terminate the pipeline gracefully if the API is not accessible.
# 
# ---
# 
# ## Workflow
# 
# 1. **Define the API Endpoint**:
#    - Specify the API URL for earthquake data.
# 
# 2. **Perform URL Validation**:
#    - Send a `HEAD` request to the API to check its status.
#    - Validate the response:
#      - **Success (HTTP 200)**: Log that the API is accessible and proceed to the next step in the pipeline.
#      - **Failure (Non-200 HTTP Status)**: Log the issue and terminate the pipeline.
# 
# 3. **Handle Errors**:
#    - Catch exceptions during the validation process, log the error, and terminate the pipeline if the validation fails.
# 
# ---
# 
# ## Outputs
# 
# 1. **Validation Log**:
#    - Logs indicating whether the API is accessible or not, along with the HTTP status code.
# 
# 2. **Pipeline Status**:
#    - A successful validation allows the pipeline to proceed to the next step.
#    - A failed validation gracefully terminates the pipeline to avoid unnecessary processing.
# 
# ---
# 
# ## Notes
# 
# - **Error Management**:
#   - Ensures errors are logged and prevents the pipeline from proceeding with invalid or inaccessible URLs.
# 
# - **Scalability**:
#   - Can be extended to validate multiple endpoints if required by future pipeline enhancements.
# 
# - **Prerequisite for Data Extraction**:
#   - This step ensures that the API is functional before attempting data ingestion, saving resources and avoiding downstream failures.


# CELL ********************

import requests
import sys

def validate_url(url):
    try:
        response = requests.head(url)
        if response.status_code == 200:
            print(f"URL {url} is accessible.")
            return True
        else:
            print(f"URL {url} is not accessible. Status Code: {response.status_code}")
            return False
    except Exception as e:
        print(f"Error while accessing URL: {e}")
        return False

url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson"
if validate_url(url):
    print("Proceeding to data extraction.")

else:
    print("Terminating pipeline due to URL validation failure.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
