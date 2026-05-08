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
# META         },
# META         {
# META           "id": "c3abadd9-077d-48dd-a313-40970c5853b4"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#Marca de agua:

df_wmo = spark.sql("select max(time) as wmo from Bronze_Landing.minciencia")
display(df_wmo)

# Ajuste directo sobre el script funcional
df_wmd = spark.sql("""
    SELECT MAX(time) AS wmd 
    FROM delta.`abfss://DP700@onelake.dfs.fabric.microsoft.com/Silver_Refined.Lakehouse/Tables/dbo/minciencia_incremental_pipeline`
""")
display(df_wmd)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Eliminar algunos datos de octubre y noviembre de la tabla destino:
# Preparar la tabla eliminando datos de octubre, noviembre y diciembre
# Usar la ruta física ABFSS para evitar la "ceguera" del motor SQL

script_delete = """
DELETE FROM delta.`abfss://DP700@onelake.dfs.fabric.microsoft.com/Silver_Refined.Lakehouse/Tables/dbo/minciencia_incremental_pipeline`
WHERE month(time) IN (10, 11, 12)
"""

# Ejecutar la instrucción de borrado
spark.sql(script_delete)

# Verificación: Calculamos la nueva fecha máxima en la tabla de destino
df_wmd_2 = spark.sql("""
    SELECT MAX(time) AS wmd_despues_del_borrado 
    FROM delta.`abfss://DP700@onelake.dfs.fabric.microsoft.com/Silver_Refined.Lakehouse/Tables/dbo/minciencia_incremental`
""")

display(df_wmd_2)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Asegurar que el nombre de la tabla sea tratado correctamente
nombre_tabla = "Bronze_Landing.marca_de_agua"

# 2. Escritura con el estándar de Fabric
# Usar format("delta") para asegurar la integridad de la tabla
df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("mergeSchema", "true") \
  .saveAsTable(nombre_tabla)

print(f"Control de Calidad: Tabla {nombre_tabla} actualizada exitosamente.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%sql
# SELECT MAX(time) AS Fecha_Maxima
# FROM delta.`abfss://DP700@onelake.dfs.fabric.microsoft.com/Silver_Refined.Lakehouse/Tables/dbo/minciencia_incremental_pipeline`

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# %%sql
# SELECT MAX(time) AS Fecha_Maxima 
# FROM minciencia

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
