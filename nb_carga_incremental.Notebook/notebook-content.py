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
# META           "id": "c3abadd9-077d-48dd-a313-40970c5853b4"
# META         },
# META         {
# META           "id": "f2f5ef99-fa5e-4802-b052-60d3674c2af2"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Notebook para carga incremental

# Buscar datos de origen usando la ruta física directa:
df_origen = spark.sql("""
    SELECT max(time) as fecha_max_wm_origen FROM delta.`abfss://DP700@onelake.dfs.fabric.microsoft.com/Bronze_Landing.Lakehouse/Tables/minciencia`
""")

display(df_origen)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Marca de agua o máximo existente usando la ruta física que sí es visible:
df_wm = spark.sql("""
    SELECT max(time) as fecha_max_wm_destino FROM delta.`abfss://DP700@onelake.dfs.fabric.microsoft.com/Silver_Refined.Lakehouse/Tables/dbo/minciencia_incremental_pipeline`
""")

display(df_wm)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
