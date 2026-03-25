# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f314a57b-c3af-4b49-a101-14ffddc0dafa",
# META       "default_lakehouse_name": "LH_Data_Landingzone",
# META       "default_lakehouse_workspace_id": "f90e9fab-2433-4a89-8266-d1bef4255c30",
# META       "known_lakehouses": [
# META         {
# META           "id": "f314a57b-c3af-4b49-a101-14ffddc0dafa"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import expr
from pyspark.sql import functions as F
from pyspark.sql.functions import base64
import numpy as np
from delta.tables import DeltaTable


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # CSV
# PrimaryKeys = "Index"
# SourceFileType = 'csv'
# IsIncremental = False

# SourceFilePath = "LDZ_PROCESS/2024/01/03"
# SourceFileName = "organizations-100.txt_202401030820.txt"

# TargetSchema = 'LDZ'
# TargetName = 'Organizations'

#abfss://f90e9fab-2433-4a89-8266-d1bef4255c30@onelake.dfs.fabric.microsoft.com/b9550f3f-b45b-4147-aef4-58ca8b1a252e/Tables/export_opendata_lrk

# EntityValues = """{"ColumnDelimiter": ",", "RowDelimiter": "\\r\\n", "Encoding": "UTF-8", "EscapeCharacter": "\\\\", "QuoteCharacter": "\\"", "FirstRowIsHeader": 1, "RemoveFile": 0}"""

# Column_Mapping = """[{"SourceColumn":"Index","TargetColumn":"Index","DataTypeOverride":"Integer"},{"SourceColumn":"Organization_Id","TargetColumn":"Organization_Id","DataTypeOverride":"String"},{"SourceColumn":"Name","TargetColumn":"Name","DataTypeOverride":"String"},{"SourceColumn":"Website","TargetColumn":"Website","DataTypeOverride":"String"},{"SourceColumn":"Country","TargetColumn":"Country","DataTypeOverride":"String"},{"SourceColumn":"Description","TargetColumn":"Description","DataTypeOverride":"String"},{"SourceColumn":"Founded","TargetColumn":"Founded","DataTypeOverride":"Integer"},{"SourceColumn":"Industry","TargetColumn":"Industry","DataTypeOverride":"String"},{"SourceColumn":"Number_of_employees","TargetColumn":"Number_of_employees","DataTypeOverride":"Integer"}]"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Parameters


# CELL ********************

# # CSV
SourceFileType = 'csv'

SourceFilePath = "Files/LRK/"
SourceFileName = "export_opendata_lrk.csv"
source_workspace = "f90e9fab-2433-4a89-8266-d1bef4255"
source_lakehouse = "b9550f3f-b45b-4147-aef4-58ca8b1a252e"
source_data_source ="LRK"
source_schema = "LRK"
source_name ="export_opendata_lrk_2"

TargetSchema = ''
TargetName = ''

EntityValues = ""

Column_Mapping = ""

source_lakehouse_path = "LH_Bronze_Layer"
relative_path_to_source = ""

#CSV arguments
CompressionType = 'infer'
ColumnDelimiter = ';'
RowDelimiter = '\n'
EscapeCharacter = '"'
Encoding = 'UTF-8'

super_secret_key = '1HarryPotterAndTheSorcerersStone'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
