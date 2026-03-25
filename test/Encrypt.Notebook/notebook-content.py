# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "09e7f4e0-83a6-4f22-9585-c6281d9958e7",
# META       "default_lakehouse_name": "Lakehouse",
# META       "default_lakehouse_workspace_id": "f90e9fab-2433-4a89-8266-d1bef4255c30",
# META       "known_lakehouses": [
# META         {
# META           "id": "09e7f4e0-83a6-4f22-9585-c6281d9958e7"
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Lakehouse.export_opendata_lrk LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

super_secret_key = '1HarryPotterAndTheSorcerersStone'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_columns_to_be_encrypted = spark.read.format("jdbc").option("url", "jdbc:sqlserver://sql-dqs-fabric.database.windows.net:1433;database=db-dqs-fabric").option("dbtable", "dbo.ColumnMapping_Encrypt").option("user", "NCC").option("password", "@Gorterhof1977").load()


pdf_columns_to_be_encrypted = df_columns_to_be_encrypted.toPandas()
columns_to_be_encrypted = pdf_columns_to_be_encrypted["TargetColumn"].values
columns_to_be_decrypted = pdf_columns_to_be_encrypted["DecryptColumn"].values


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

encrypt_cols = []

for table_columns in df.columns:
    if table_columns in columns_to_be_encrypted:
        encrypt_cols.append(expr(f"aes_encrypt({table_columns}, '{super_secret_key}', 'ECB')").alias(f"{table_columns}_Hashed"))
        encrypt_cols.append(base64(f"{table_columns}_Hashed").alias(f"{table_columns}_Encrypted"))
    else:
        encrypt_cols.append(expr(f"{table_columns}"))

df_encrypted = df\
    .select(encrypt_cols)

df_encrypted.show
display(df_encrypted)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

decrypt_cols = []

for table_columns in df_encrypted.columns:
    if table_columns in columns_to_be_decrypted:
        decrypt_cols.append(expr(f"aes_decrypt(unbase64({table_columns}), '{super_secret_key}', 'ECB')").cast('string').alias(f"{table_columns}_Decrypted"))

    else:
        decrypt_cols.append(expr(f"{table_columns}"))

df_decrypted = df_encrypted\
    .select(decrypt_cols)


display(df_decrypted)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

masking_cols = []
df_email = df_columns_to_be_encrypted
display(df_email)



df_masking = df.withColumn('contact_emailadres', F.regexp_replace('contact_emailadres', '(?<!^).(?=.+@)', '*'))
display(df_masking)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
