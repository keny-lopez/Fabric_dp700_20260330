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

# Un poquito de SPARK en Notebook:

DataframeSpark = spark.read.table("Bronze_Landing.minciencia")
df_spark_filtrado = DataframeSpark.select(
    "time",
    "ff_valor",
    "CodigoNacional"
)

# display(DataframeSpark)
display(df_spark_filtrado)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
from deltalake import DeltaTable

# 1. Ruta de File API (Más estable para librerías de Python puro en Fabric)
# Sustituye con la ruta que obtienes haciendo clic derecho en la tabla -> 'Propiedades' -> 'Ruta de API de archivo'
path = "/lakehouse/default/Tables/minciencia"

# 2. Lectura que respeta el registro de transacciones (_delta_log)
# Esto garantiza que NO leas datos fantasma o archivos parquet obsoletos
dt = DeltaTable(path)
df_pandas = dt.to_pandas()

display(df_pandas.head())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
