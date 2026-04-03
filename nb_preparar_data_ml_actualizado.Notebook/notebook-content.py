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
# META           "id": "4e079e43-45c5-42e9-bc33-558028f6866d"
# META         },
# META         {
# META           "id": "c3abadd9-077d-48dd-a313-40970c5853b4"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 📓 nb_preparar_data_ml_actualizado
# ## Preparación de datos para ML · Silver → Gold
# > Versión corregida del script original del instructor (13/11/2025).
# > Corrige errores de tipeo en nombres de columna, nomenclatura
# > del catálogo de Spark y opciones de escritura Delta.
# > Lakehouse predeterminado: Gold_Trusted · Secundario: Silver_Refined

# CELL ********************

# 📓 nb_preparar_data_ml_actualizado
## Preparación de datos para ML · Silver → Gold
> Versión corregida del script original del instructor (13/11/2025).
> Corrige errores de tipeo en nombres de columna, nomenclatura
> del catálogo de Spark y opciones de escritura Delta.
> Lakehouse predeterminado: Gold_Trusted · Secundario: Silver_Refined


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================
# ⚠️  NOTEBOOK CORREGIDO — REFERENCIA COMPARATIVA
#
# Aplica correcciones mínimas al script del instructor para
# funcionar en Fabric abril 2026. No incorpora estándares
# de producción — conserva la estructura original.
#
# Correcciones aplicadas:
#   · Referencia de tabla Silver sin prefijo Silver_Refined
#     — las tablas viven bajo dbo, no bajo Silver_Refined
#   · payfrequency, departmentname, groupname, departmentid
#     — cuatro nombres de columna corregidos (errores de tipeo)
#   · overwriteSchema con mayúscula S
#     — Spark es sensible a mayúsculas en opciones Delta
#   · Verificación final con spark.sql en lugar de %%sql
#     — %%sql no resuelve Gold_Trusted sin configuración adicional
#
# Para la versión de producción ver: nb_preparar_data_ml
# =============================================================

raise Exception("⛔ Notebook de referencia. Ejecución bloqueada intencionalmente.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Preparar data para ML analítica prescriptiva

df = spark.read.table("ft_costos_rrhh_dia_turno_depto_business_entity")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC   A.businessentityid                    AS idtrabajador
# MAGIC , A.shiftid                             AS idturno
# MAGIC , CAST(A.ratechangedate AS date)        AS fechahora
# MAGIC , A.rate                                AS tasa
# MAGIC , A.payfrequency                        AS frecpago
# MAGIC , A.costo_dia
# MAGIC , B.departmentname                      AS departamento
# MAGIC , B.groupname                           AS grupo
# MAGIC FROM ft_costos_rrhh_dia_turno_depto_business_entity A
# MAGIC INNER JOIN dim_departamento B
# MAGIC ON B.departmentid = A.departmentid

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Integrar Spark con SQL para crear DataFrame de entrenamiento

df_entrenamiento = spark.sql(
"""
SELECT
  A.businessentityid                    AS idtrabajador
, A.shiftid                             AS idturno
, CAST(A.ratechangedate AS date)        AS fechahora
, A.rate                                AS tasa
, A.payfrequency                        AS frecpago
, A.costo_dia
, B.departmentname                      AS departamento
, B.groupname                           AS grupo
FROM ft_costos_rrhh_dia_turno_depto_business_entity A
INNER JOIN dim_departamento B
ON B.departmentid = A.departmentid
"""
)

display(df_entrenamiento)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Guardar DataFrame de entrenamiento en capa Gold

df_entrenamiento.write\
    .format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable("Gold_Trusted.train_tasatrabajo")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verificación

spark.sql("SELECT * FROM Gold_Trusted.train_tasatrabajo").show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
