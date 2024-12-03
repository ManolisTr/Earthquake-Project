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

# MARKDOWN ********************

# # Preprocessing Notebook: Pipeline Check and Cleanup
# 
# This notebook serves as a pre-ingestion step in the pipeline. It verifies whether the pipeline has already processed data for the specified date and cleans up existing records in the Silver and Gold Layers if necessary. This ensures a clean state for reprocessing and prevents data duplication.
# 
# ---
# 
# ## Objectives
# 
# 1. **Pipeline Execution Check**:
#    - Verify if data for the specified date (yesterday) already exists in the Silver Layer.
# 
# 2. **Data Cleanup**:
#    - If data exists, remove records for the specified date from both the Silver and Gold Layers.
# 
# 3. **Prepare for Ingestion**:
#    - If no data exists, confirm that the pipeline is ready to process new data.
# 
# ---
# 
# ## Workflow
# 
# 1. **Define Processing Date**:
#    - Dynamically determine the date for which data needs to be checked (yesterday's date).
# 
# 2. **Check Existing Data**:
#    - Query the Silver Layer to check if earthquake data for the specified date already exists.
#    - Count the records matching the date to determine if the pipeline has already processed this data.
# 
# 3. **Delete Existing Records**:
#    - If data exists:
#      - Remove all records for the specified date from the Silver Layer.
#      - Remove all records for the specified date from the Gold Layer.
#    - Log the status of deletions.
# 
# 4. **Proceed to Ingestion**:
#    - If no data exists for the specified date, confirm readiness for ingestion and log this status.
# 
# ---
# 
# ## Outputs
# 
# 1. **Status Logs**:
#    - Logs indicating whether data for the specified date was found and deleted, or if the pipeline is ready for ingestion.
# 
# 2. **Clean State**:
#    - Both the Silver and Gold Layers are cleaned of records for the specified date if they previously existed.
# 
# ---
# 
# ## Notes
# 
# - **Dynamic Date Handling**:
#   - Automatically calculates the date to check (yesterday's date) to ensure flexibility and reduce manual effort.
# 
# - **Error Prevention**:
#   - Prevents duplication of data by cleaning up records before reprocessing.
# 
# - **Scalability**:
#   - Designed to handle datasets of varying sizes efficiently, ensuring quick validation and cleanup.
# 
# - **Prerequisite for Ingestion**:
#   - Ensures the pipeline is always working with a clean state, ready for fresh data ingestion.


# CELL ********************

from pyspark.sql import SparkSession
from datetime import date, timedelta

# Initialize Spark session (if not already done)
spark = SparkSession.builder.getOrCreate()

# Today's date
today_date = date.today()-timedelta(1)

# Check if data already exists in the Silver table
query = f"""
SELECT COUNT(*) as record_count 
FROM earthquake_events_silver 
WHERE CAST(time as date) = '{today_date}'
"""
result_df = spark.sql(query)
record_count = result_df.collect()[0]['record_count']

if record_count > 0:
    print(f"Pipeline has already run for {today_date}. Proceeding to delete existing data.")

    # Delete data from the Silver table
    spark.sql(f"""
    DELETE FROM earthquake_events_silver
    WHERE CAST(time as date) = '{today_date}'
    """)
    print("Data deleted from Silver table.")

    # Delete data from the Gold table
    spark.sql(f"""
    DELETE FROM earthquake_events_gold
    WHERE CAST(time as date) = '{today_date}'
    """)
    print("Data deleted from Gold table.")
else:
    print(f"No data found for {today_date}. Proceeding with ingestion.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
