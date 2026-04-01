# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c3abadd9-077d-48dd-a313-40970c5853b4",
# META       "default_lakehouse_name": "Silver_Refined",
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

# MARKDOWN ********************

# # Clase 2 - 06/11/2025
# ## Análisis RRHH — Código del Instructor Corregido para Fabric 2026
# > Este notebook replica el ejercicio original del instructor con los
# > ajustes mínimos necesarios para funcionar en Fabric a partir de 2026.
# > No incorpora estándares de producción — conserva la estructura original.

# CELL ********************

# =============================================================
# ⚠️  NOTEBOOK DE REFERENCIA — NO EJECUTAR
#
# Este notebook conserva el código del instructor corregido
# para Fabric 2026 con fines comparativos y pedagógicos.
#
# Ejecutarlo generaría tablas duplicadas en Bronze_Landing
# y Silver_Refined porque esas tablas ya existen desde
# nb_analisis_rrhh (notebook de producción).
#
# Para estudiar las diferencias entre versiones, comparar
# este notebook contra nb_analisis_rrhh celda por celda.
# =============================================================

raise Exception("⛔ Notebook de referencia. Ejecución bloqueada intencionalmente.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dependencias

# CELL ********************

# Instalación de fastparquet para resolver el tipo
# INT64 (TIME(NANOS,true)) de la entidad shift.
# El instructor no necesitaba esto en noviembre 2025
# porque usaba una versión anterior del motor de Fabric.

%pip install fastparquet


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Parámetros de Rutas ABFS

# CELL ********************

# Corrección 1: El instructor usaba rutas relativas como
# "Files/humanresources/department". En 2026 esto falla
# cuando Silver_Refined es el lakehouse predeterminado
# porque la ruta relativa apunta a Silver, no a Bronze.
# Se reemplaza por rutas ABFS absolutas con GUID.

WORKSPACE_ID  = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"
BRONZE_ID     = "f2f5ef99-fa5e-4802-b052-60d3674c2af2"

BRONZE_BASE = (
    f"abfss://{WORKSPACE_ID}"
    f"@onelake.dfs.fabric.microsoft.com"
    f"/{BRONZE_ID}/Files/humanresources"
)

print(f"✅ BRONZE_BASE → {BRONZE_BASE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Lectura de ficheros Parquet desde Bronze_Landing

# CELL ********************

# Corrección 1 aplicada: rutas ABFS absolutas en lugar
# de rutas relativas "Files/humanresources/..."

from pyspark.sql import functions as F

df_department     = spark.read.parquet(f"{BRONZE_BASE}/department")
df_employeedpthis = spark.read.parquet(f"{BRONZE_BASE}/employeedepartmenthistory")
df_employeepayhistory = spark.read.parquet(f"{BRONZE_BASE}/employeepayhistory")
df_jobcandidate   = spark.read.parquet(f"{BRONZE_BASE}/jobcandidate")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Guardar DataFrames como tablas Delta en Bronze_Landing

# CELL ********************

# Sin cambios respecto al instructor.
# saveAsTable con nombre explícito de schema funciona
# correctamente en 2026.

df_department.write.format("delta").mode("overwrite").saveAsTable("Bronze_landing.hr_department")
df_employeedpthis.write.format("delta").mode("overwrite").saveAsTable("Bronze_landing.hr_employeedepartmenthistory")
df_employeepayhistory.write.format("delta").mode("overwrite").saveAsTable("Bronze_landing.hr_employeepayhistory")
df_jobcandidate.write.format("delta").mode("overwrite").saveAsTable("Bronze_landing.hr_jobcandidate")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Tratamiento especial: entidad shift

# CELL ********************

# Corrección 2: El instructor usaba pd.read_parquet con
# la ruta local /lakehouse/default/Files/humanresources/shift
# que en 2026 falla porque:
#   1. /lakehouse/default apunta a Silver, no a Bronze
#   2. PyArrow rechaza el tipo TIME(NANOS,true) de PostgreSQL
#
# Se reemplaza por fastparquet con ruta ABFS absoluta.

import pandas as pd

# 1) Lee con fastparquet desde ABFS
df_shift_pandas = pd.read_parquet(
    f"{BRONZE_BASE}/shift",
    engine="fastparquet"
)

df_shift_pandas = df_shift_pandas.drop(columns=["modifieddate"])

# 2) Normaliza tipos: time -> string
for c in ["starttime", "endtime"]:
    df_shift_pandas[c] = df_shift_pandas[c].astype(str)

# 3) A Spark
df_shift = spark.createDataFrame(df_shift_pandas)

# 4) Casts finales
df_shift = (df_shift
  .withColumn("shiftid",    F.col("shiftid").cast("int"))
  .withColumn("name",       F.col("name").cast("string"))
  .withColumn("starttime",  F.col("starttime").cast("string"))
  .withColumn("endtime",    F.col("endtime").cast("string"))
)

# 5) Guarda como tabla Delta en Bronze
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze_landing")
(df_shift.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .saveAsTable("bronze_landing.hr_shift")
)

spark.sql("SELECT * FROM bronze_landing.hr_shift").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Tabla de Hechos

# CELL ********************

# Sin cambios respecto al instructor.

df_ft_costos_rrhh = spark.sql(
"""
SELECT
  edh.businessentityid,
  edh.departmentid,
  edh.shiftid,
  eph.ratechangedate,
  eph.rate,
  eph.payfrequency,
  CASE
    WHEN eph.payfrequency = 1 THEN eph.rate / 30
    WHEN eph.payfrequency = 2 THEN eph.rate / 15
    ELSE eph.rate
  END AS costo_dia
FROM Bronze_Landing.hr_employeedepartmenthistory edh
LEFT JOIN Bronze_Landing.hr_employeepayhistory eph
  ON edh.businessentityid = eph.businessentityid
ORDER BY
  edh.businessentityid,
  eph.ratechangedate;
"""
)
display(df_ft_costos_rrhh)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Guardar Tabla de Hechos y Dimensiones en Silver_Refined

# CELL ********************

# Sin cambios respecto al instructor.

df_ft_costos_rrhh.write.format("delta").mode("overwrite").saveAsTable("Silver_Refined.ft_costos_rrhh_dia_turno_depto_business_entity")

df_dim_trabajador = spark.sql(
"""
SELECT
  businessentityid,
  MIN(startdate) AS fechainicio,
  MAX(enddate)   AS fechatermino
FROM Bronze_Landing.hr_employeedepartmenthistory
GROUP BY businessentityid
"""
)
display(df_dim_trabajador)

df_dim_departamento = spark.sql(
"""
SELECT
  departmentid,
  name AS departmentname,
  groupname
FROM Bronze_Landing.hr_department;
"""
)
display(df_dim_departamento)

df_dim_trabajador.write.mode("overwrite").saveAsTable("Silver_Refined.dim_trabajador")
df_dim_departamento.write.mode("overwrite").saveAsTable("Silver_Refined.dim_departamento")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dimensión Turno

# CELL ********************

# Sin cambios respecto al instructor.

df_dim_turno = spark.sql(
"""
SELECT
  shiftid,
  name      AS shiftname,
  starttime,
  endtime
FROM Bronze_Landing.hr_shift;
"""
)
display(df_dim_turno)

df_dim_turno.write.mode("overwrite").saveAsTable("Silver_Refined.dim_turno")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
