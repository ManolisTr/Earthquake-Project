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

# # Cold Layer Notebook: Earthquake Events Processing
# 
# This notebook processes data from the Silver Layer to generate a refined dataset for the Gold Layer. It enriches the data with geolocation details and classifies earthquake events based on their significance.
# 
# ---
# 
# ## Objectives
# 
# 1. **Geographical Enrichment**:
#    - Add the `country_code` to each earthquake event by performing reverse geocoding using latitude and longitude.
# 
# 2. **Significance Classification**:
#    - Classify earthquake events into `Low`, `Moderate`, or `High` significance categories based on their `sig` value.
# 
# 3. **Data Output**:
#    - Append the enriched and classified data to the `earthquake_events_gold` table for further analysis.
# 
# ---
# 
# ## Workflow
# 
# ### 1. Load Data
# - Data is read from the Silver Layer table (`earthquake_events_silver`) as the input for processing.
# 
# ### 2. Reverse Geocoding
# - The `latitude` and `longitude` of each earthquake event are used to derive the `country_code` via reverse geocoding.
# - Batch processing is used to improve performance and reduce computational overhead.
# 
# ### 3. Significance Classification
# - Each earthquake event is categorized into one of three significance levels:
#   - **Low**: For events with a `sig` value below 100.
#   - **Moderate**: For events with a `sig` value between 100 and 499.
#   - **High**: For events with a `sig` value of 500 or more.
# 
# ### 4. Logging and Metrics
# - The number of records processed is logged for monitoring purposes.
# - Additional logging may capture the success or failure of key steps.
# 
# ### 5. Write to Gold Layer
# - The processed data, including the enriched `country_code` and `sig_class`, is appended to the `earthquake_events_gold` table in the Gold Layer.
# 
# ---
# 
# ## Parameters
# 
# | Parameter         | Description                                        |
# |--------------------|----------------------------------------------------|
# | `latitude`         | Latitude of the earthquake location               |
# | `longitude`        | Longitude of the earthquake location              |
# | `sig`              | Significance of the earthquake event              |
# | `country_code`     | Country code derived from reverse geocoding        |
# | `sig_class`        | Classification based on the significance of event |
# 
# ---
# 
# ## Outputs
# 
# The Gold Layer table, `earthquake_events_gold`, contains the enriched data with the following new columns:
# 1. **`country_code`**: The country where the earthquake occurred.
# 2. **`sig_class`**: The significance classification (`Low`, `Moderate`, `High`).
# 
# ---
# 
# ## Notes
# 
# - **Reverse Geocoding**:
#   - Batch processing is used to optimize geocoding and improve efficiency.
#   - The `reverse_geocoder` library is used to derive the country code.
# 
# - **Significance Thresholds**:
#   - The thresholds for significance classification can be adjusted based on the use case.
# 
# - **Performance Considerations**:
#   - Ensure efficient execution by leveraging Spark’s parallel processing capabilities for batch operations.
# 
# - **Monitoring**:
#   - Logs for record counts and processing steps are included to track pipeline performance.


# CELL ********************

from pyspark.sql.functions import when, col, lit, udf, pandas_udf, PandasUDFType
from pyspark.sql.types import StringType
import reverse_geocoder as rg
import pandas as pd

# Read data from the Silver table
df = spark.read.table("earthquake_events_silver")

# Helper function for batch reverse geocoding
def batch_reverse_geocode(latitudes, longitudes):
    """
    Perform reverse geocoding for a batch of latitude and longitude values.

    Parameters:
    latitudes (pd.Series): Series of latitude values.
    longitudes (pd.Series): Series of longitude values.

    Returns:
    pd.Series: Series of country codes corresponding to the input coordinates.
    """
    coordinates = list(zip(latitudes, longitudes))
    results = rg.search(coordinates)
    return pd.Series([res.get('cc', 'Unknown') for res in results])

# Register a pandas UDF for batch reverse geocoding
@pandas_udf(StringType(), PandasUDFType.SCALAR)
def batch_reverse_geocode_udf(latitudes: pd.Series, longitudes: pd.Series) -> pd.Series:
    return batch_reverse_geocode(latitudes, longitudes)

# Add country_code column
df_with_location = df.withColumn(
    "country_code",
    batch_reverse_geocode_udf(col("latitude"), col("longitude"))
)

# Define significance thresholds
low_threshold = 100
moderate_threshold = 500

# Add significance classification
df_with_location_sig_class = df_with_location.withColumn(
    "sig_class",
    when(col("sig") < lit(low_threshold), "Low")
    .when((col("sig") >= lit(low_threshold)) & (col("sig") < lit(moderate_threshold)), "Moderate")
    .otherwise("High")
)

# Log record count
record_count = df_with_location_sig_class.count()
print(f"Number of records processed: {record_count}")

# Write to the Gold table
df_with_location_sig_class.write.mode('append').saveAsTable('earthquake_events_gold')

print("Data successfully written to the Gold table: earthquake_events_gold")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
