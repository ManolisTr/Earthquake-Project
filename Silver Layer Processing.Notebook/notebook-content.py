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

# # Silver Layer Notebook: Earthquake Data Transformation
# 
# This notebook processes raw earthquake data from the Bronze Layer, applies necessary transformations, and writes the refined data to the Silver Layer. It ensures data quality and prepares the data for further enrichment in the Cold Layer.
# 
# ---
# 
# ## Objectives
# 
# 1. **Load and Parse Data**:
#    - Dynamically identify and read the raw earthquake data file saved by the Bronze Layer, based on the processing date (two days prior).
#    - Validate that the file is successfully loaded and ready for transformation.
# 
# 2. **Transform Data**:
#    - Extract key attributes such as `longitude`, `latitude`, `elevation`, `place_description`, `significance`, `magnitude`, and timestamp details.
#    - Convert timestamp fields from milliseconds to a human-readable `datetime` format for easier analysis.
# 
# 3. **Data Quality and Deduplication**:
#    - Perform a data quality check to ensure that the dataset contains records.
#    - Remove duplicate records to maintain data integrity, using unique identifiers.
# 
# 4. **Write to Silver Layer**:
#    - Append the transformed and deduplicated data to the `earthquake_events_silver` table in the Silver Layer.
# 
# 5. **Log Metrics**:
#    - Log the number of records processed after transformation and deduplication for monitoring and auditing.
# 
# ---
# 
# ## Workflow
# 
# 1. **Loading Data**:
#    - Dynamically calculate the file path based on the processing date and read the raw JSON data.
# 
# 2. **Transforming Data**:
#    - Extract key attributes required for downstream processes.
#    - Convert raw timestamps to a standardized format for better readability.
# 
# 3. **Ensuring Data Quality**:
#    - Check for the presence of records in the dataset.
#    - Deduplicate records to avoid redundant entries in the Silver Layer.
# 
# 4. **Saving Data**:
#    - Append the cleaned and enriched data to the `earthquake_events_silver` table.
# 
# 5. **Logging**:
#    - Capture key metrics such as the number of processed and deduplicated records to ensure process visibility and traceability.
# 
# ---
# 
# ## Outputs
# 
# The processed data in the Silver Layer includes the following attributes:
# 1. **Geospatial Data**:
#    - Longitude, Latitude, Elevation.
# 2. **Event Details**:
#    - Title, Place Description, Significance (`sig`), Magnitude (`mag`), Magnitude Type (`magType`).
# 3. **Timestamps**:
#    - Event Time, Updated Time (converted to human-readable format).
# 
# ---
# 
# ## Notes
# 
# - **Dynamic File Loading**:
#   - The notebook automatically identifies the raw data file saved by the Bronze Layer based on the processing date.
# 
# - **Data Integrity**:
#   - Deduplication ensures no duplicate records are introduced into the Silver Layer.
# 
# - **Error Handling**:
#   - Gracefully handles errors during data loading and writing, raising appropriate exceptions if issues occur.
# 
# - **Scalability**:
#   - Designed to handle variable data sizes with efficient transformations and checks.


# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import TimestampType, StructType, StructField, StringType, DoubleType
from datetime import date, timedelta
from pyspark.sql.functions import from_json
from pyspark.sql.types import ArrayType, DoubleType


start_date = date.today()-timedelta(2)

# Initialize Spark session (if not already done)
spark = SparkSession.builder.getOrCreate()

# Define dynamic file path based on today's date
file_path = f'Files/{start_date}_earthquake_data.json'


# Read the JSON file
try:
    df = spark.read.option("multiline", "true").json(file_path)
    print(f"Successfully loaded data from {file_path}.")
except Exception as e:
    raise Exception(f"Error reading JSON file: {e}")


df_transformed = (
    df.select(
        'id',
        col('geometry.coordinates').getItem(0).alias('longitude'),
        col('geometry.coordinates').getItem(1).alias('latitude'),
        col('geometry.coordinates').getItem(2).alias('elevation'),
        col('properties.title').alias('title'),
        col('properties.place').alias('place_description'),
        col('properties.sig').alias('sig'),
        col('properties.mag').alias('mag'),
        col('properties.magType').alias('magType'),
        col('properties.time').alias('time'),
        col('properties.updated').alias('updated')
        )
)

df_transformed = df_transformed.\
    withColumn('time', col('time')/1000).\
    withColumn('updated', col('updated')/1000).\
    withColumn('time', col('time').cast(TimestampType())).\
    withColumn('updated', col('updated').cast(TimestampType()))


# Data Quality Checks
record_count = df_transformed.count()
if record_count == 0:
    raise Exception("No records found in the input data. Aborting pipeline.")

# Deduplication: Remove duplicate records based on unique identifiers
df_deduplicated = df_transformed.dropDuplicates(['id'])

# Log key metrics
print(f"Number of records after transformation: {record_count}")
print(f"Number of unique records after deduplication: {df_deduplicated.count()}")

# Write to the Silver table
try:
    df_deduplicated.write.mode('append').saveAsTable('earthquake_events_silver')
    print(f"Data successfully written to the Silver table: earthquake_events_silver")
except Exception as e:
    raise Exception(f"Error writing data to Silver table: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
