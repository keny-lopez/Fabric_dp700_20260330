# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f2f5ef99-fa5e-4802-b052-60d3674c2af2",
# META       "default_lakehouse_name": "Bronze_Landing",
# META       "default_lakehouse_workspace_id": "f2f1a2a4-28bd-4388-aea2-989b8d2479ff",
# META       "known_lakehouses": [
# META         {
# META           "id": "f2f5ef99-fa5e-4802-b052-60d3674c2af2"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Notebook de ingesta:

# Invocar librerías:
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
import os
import json
import pyspark.sql.functions as F
import time

# Configuración:
ruta_origen = "Files/ficheros_json"
nombre_tabla = "temperatura_simulada"
ruta_check = "Files/ficheros_checkpoint"

# Esquema del fichero de origen (Data JSON de origen):
file_schema = StructType() \
    .add("id", StringType()) \
    .add("temp", DoubleType()) \
    .add("timestamp", TimestampType())

spark.sql(f"CREATE TABLE IF NOT EXISTS {nombre_tabla}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Leer el fichero de origen:
fichero_raiz_df = spark.readStream\
    .schema(file_schema)\
    .option("maxFilesPerTrigger", 1)\
    .json(ruta_origen)


# Agregar el timestamp de procesamiento:
fichero_mod_df = fichero_raiz_df.withColumn("ts_process", F.current_timestamp())

# Escribir la data en la tabla Delta:
deltastream = fichero_mod_df\
    .writeStream\
    .format("delta")\
    .outputMode("append")\
    .option("mergeSchema", True)\
    .option("checkpointLocation", ruta_check)\
    .start(f"Tables/{nombre_tabla}")

print(fichero_raiz_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("Bronze_Landing.temperatura_simulada")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fichero_raiz_df.isStreaming

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltastream.isActive

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltastream.status

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltastream.lastProgress

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

while deltastream.isActive:
    print("✅ Stream is running...")
    print("📊 Last progress:", deltastream.lastProgress)
    time.sleep(5)
print("❌ Stream has stopped.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

deltastream.stop()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfst = spark.sql("SELECT count(*) FROM Bronze_Landing.temperatura_simulada")
display(dfst)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
