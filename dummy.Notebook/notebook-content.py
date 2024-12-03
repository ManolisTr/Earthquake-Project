# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Importing necessary libraries
import pandas as pd

# Step 1: Create dummy data
data = {
    "Name": ["Alice", "Bob", "Charlie", "Diana"],
    "Age": [25, 30, 35, 40],
    "Department": ["HR", "IT", "Finance", "Marketing"]
}

# Convert to DataFrame
df = pd.DataFrame(data)

# Step 2: Perform a simple transformation
# Filter employees older than 30
filtered_df = df[df["Age"] > 30]

# Step 3: Display the results
print("Original DataFrame:")
print(df)
print("\nFiltered DataFrame (Age > 30):")
print(filtered_df)


print('----------------------------------------------------------------------------------------')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sempy.fabric as fabric
import pandas as pd

shortcuts_id = fabric.list_workspaces().query('Name.str.contains("Earthquake Project", case=False)').Id.iloc[0]


List_items=fabric.list_items(workspace=shortcuts_id)

List_items

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspaces = fabric.list_workspaces()
print(workspaces)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
