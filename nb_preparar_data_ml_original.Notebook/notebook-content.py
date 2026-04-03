# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4e079e43-45c5-42e9-bc33-558028f6866d",
# META       "default_lakehouse_name": "Gold_Trusted",
# META       "default_lakehouse_workspace_id": "f2f1a2a4-28bd-4388-aea2-989b8d2479ff",
# META       "known_lakehouses": [
# META         {
# META           "id": "c3abadd9-077d-48dd-a313-40970c5853b4"
# META         },
# META         {
# META           "id": "4e079e43-45c5-42e9-bc33-558028f6866d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# =============================================================
# ⚠️  NOTEBOOK DE REFERENCIA — NO EJECUTAR
#
# Este notebook conserva el código original del instructor
# tal como fue presentado en clase el 13/11/2025.
#
# Contiene problemas de compatibilidad con Fabric 2026:
#   · Rutas relativas que fallan con lakehouse no predeterminado
#   · Lectura de shift con PyArrow que rechaza TIME(NANOS,true)
#   · Dependencia de /lakehouse/default apuntando a Bronze
#
# Para la versión corregida ver: nb_preparar_data_ml_actualizado
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

df = spark.read.table("Silver_Refined.ft_costos_rrhh_dia_turno_depto_business_entity")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC   A.businessentityid    as  idtrabajador
# MAGIC , A.shiftid             as  idturno
# MAGIC , Cast(A.ratechangedate as date)  as  fechahora
# MAGIC , A.rate                as  tasa
# MAGIC , A.payfrecuency        as  freppago
# MAGIC , A.costo_dia
# MAGIC , B.departamentname     as  departamento
# MAGIC , B.groupdame           as  grupo
# MAGIC FROM Silver_Refined.ft_costos_rrhh_dia_turno_depto_business_entity A
# MAGIC INNER JOIN dim_departamento B
# MAGIC ON B.departamentid = A.departamentid

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Integrar SPARK con SQL para crear un DataFrame:
df_entrenamiento = spark.sql(
"""
SELECT
  A.businessentityid    as  idtrabajador
, A.shiftid             as  idturno
, Cast(A.ratechangedate as date)  as  fechahora
, A.rate                as  tasa
, A.payfrequency        as  freqpago
, A.costo_dia
, B.departmentname      as  departamento
, B.groupname           as  grupo
FROM Silver_Refined.ft_costos_rrhh_dia_turno_depto_business_entity A
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

# Guardar DataFrame de entrenamiento en Capa Gold:

df_entrenamiento.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("Gold_Trusted.train_tasatrabajo")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verificación:

%%sql
SELECT * FROM Gold_Trusted.train_tasatrabajo

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
