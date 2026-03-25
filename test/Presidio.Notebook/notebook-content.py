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
# META     },
# META     "environment": {
# META       "environmentId": "0c9f8bec-84d7-897e-4b44-e10334619429",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

%pip install https://github.com/explosion/spacy-models/releases/download/en_core_web_lg-3.7.1/en_core_web_lg-3.7.1-py3-none-any.whl
%pip install presidio_analyzer
%pip install presidio_anonymizer

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities.engine import OperatorConfig
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import col, pandas_udf
import pandas as pd
import os

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("abfss://f90e9fab-2433-4a89-8266-d1bef4255c30@onelake.dfs.fabric.microsoft.com/09e7f4e0-83a6-4f22-9585-c6281d9958e7/Files/Titanic/titanic.csv")
# df now is a Spark DataFrame containing CSV data from "abfss://f90e9fab-2433-4a89-8266-d1bef4255c30@onelake.dfs.fabric.microsoft.com/09e7f4e0-83a6-4f22-9585-c6281d9958e7/Files/Titanic/titanic.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

analyzer = AnalyzerEngine()
anonymizer = AnonymizerEngine()
broadcasted_analyzer = sc.broadcast(analyzer)
broadcasted_anonymizer = sc.broadcast(anonymizer)
 
# define a pandas UDF function and a series function over it.
# Note that analyzer and anonymizer are broadcasted.
 
def anonymize_text(text: str) -> str:
    analyzer = broadcasted_analyzer.value
    anonymizer = broadcasted_anonymizer.value
    analyzer_results = analyzer.analyze(text=text, language="en")
    anonymized_results = anonymizer.anonymize(
        text=text,
        analyzer_results=analyzer_results,
        operators={"DEFAULT": OperatorConfig("encrypt", {"key": encryption_key})},
    )
    return anonymized_results.text
 
def anonymize_series(s: pd.Series) -> pd.Series:
    return s.apply(anonymize_text)
 
# define a the function as pandas UDF
anonymize = pandas_udf(anonymize_series, returnType=StringType()) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

anonymized_column = "Name"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# apply the udf
anonymized_df = df.withColumn(
    anonymized_column, anonymize(col(anonymized_column))
)
display(anonymized_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
