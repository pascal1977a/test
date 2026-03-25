# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b9550f3f-b45b-4147-aef4-58ca8b1a252e",
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": [
# META         {
# META           "id": "b9550f3f-b45b-4147-aef4-58ca8b1a252e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# CELL ********************

df = spark.sql("SELECT * FROM LH_Bronze_Layer.export_opendata_lrk LIMIT 1000")
display(df)

abfss://f90e9fab-2433-4a89-8266-d1bef4255c30@onelake.dfs.fabric.microsoft.com/b9550f3f-b45b-4147-aef4-58ca8b1a252e/Tables/export_opendata_lrk
