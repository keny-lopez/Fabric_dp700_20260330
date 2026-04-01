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
# META           "id": "f2f5ef99-fa5e-4802-b052-60d3674c2af2"
# META         },
# META         {
# META           "id": "c3abadd9-077d-48dd-a313-40970c5853b4"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Clase 3 - 11/11/2025
# ## Sales · CurrencyRate — Código Original del Instructor
# > Este notebook es una réplica exacta del ejercicio original.
# > Contiene problemas de compatibilidad con Fabric 2026.
# > Conservado con fines comparativos y pedagógicos.

# CELL ********************

# =============================================================
# ⚠️  NOTEBOOK DE REFERENCIA — NO EJECUTAR
#
# Este notebook conserva el código original del instructor
# tal como fue presentado en clase el 11/11/2025.
#
# Problemas de compatibilidad con Fabric 2026:
#   · Ruta relativa /lakehouse/default/Files/sales/currencyrate
#     falla cuando Silver_Refined es el lakehouse predeterminado
#     porque apunta a Silver, no a Bronze.
#
# Para la versión de producción ver: nb_sales_bronze
# =============================================================

raise Exception("⛔ Notebook de referencia. Ejecución bloqueada intencionalmente.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Manipular el fichero Parquet para transformarlo en Tabla

# CELL ********************

# 1) leer desde pandas el fichero parquet
import pandas as pd

df_cr_pandas = pd.read_parquet("/lakehouse/default/Files/sales/currencyrate")
df_cr_pandas = df_cr_pandas.drop(columns=["modifieddate"])
# display(df_cr_pandas)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2) transformar de pandas a spark
from pyspark.sql import functions as F
df_cr = spark.createDataFrame(df_cr_pandas)

# castear el dataframe
df_cr = (df_cr
  .withColumn("currencyrateid",   F.col("currencyrateid").cast("int"))
  .withColumn("currencyratedate", F.col("currencyratedate").cast("date"))
  .withColumn("fromcurrencycode", F.col("fromcurrencycode").cast("string"))
  .withColumn("tocurrencycode",   F.col("tocurrencycode").cast("string"))
  .withColumn("averagerate",      F.col("averagerate").cast("double"))
  .withColumn("endofdayrate",     F.col("endofdayrate").cast("double"))
)
# display(df_cr)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3) guardar de spark a tabla en el lakehouse
df_cr.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("Bronze_Landing.sales_currencyrate")

# verificacion
spark.sql("SELECT * FROM Bronze_Landing.sales_currencyrate")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
