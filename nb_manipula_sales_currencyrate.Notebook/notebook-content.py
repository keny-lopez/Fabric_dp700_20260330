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

# MARKDOWN ********************

# # 🏗️ Bronze · Dominio Sales — Tratamiento de Entidad Especial
# ## Workspace DP700 · AdventureWorks Sales
# 
# ---
# 
# ### Contexto y propósito
# 
# Este notebook opera exclusivamente dentro de **Bronze_Landing**.
# Origen y destino son el mismo lakehouse — Silver_Refined no interviene.
# 
# #### ¿Por qué existe este notebook?
# 
# El pipeline `pl_sales_aw` mueve 10 entidades del dominio `sales` desde
# PostgreSQL (AdventureWorks) hacia Bronze_Landing mediante dos actividades:
# 
# - `Copy job_f0d` — transporta 9 entidades directamente como tablas Delta.
# - `Copy data 1` — transporta `currencyrate` en formato Parquet porque sus
#   columnas `DECIMAL` superan la precisión máxima que Delta acepta, lo que
#   genera el error `DeltaInvalidBigDecimalPrecisionScale` al intentar
#   escribirla como Delta directamente desde el pipeline.
# 
# Este notebook recoge el Parquet depositado por `Copy data 1`, castea las
# columnas problemáticas a `double` y persiste `currencyrate` como tabla
# Delta, dejándola en igualdad de condiciones con las otras 9 entidades.
# 
# #### Etapa previa requerida
# 
# Antes de ejecutar este notebook deben haberse completado exitosamente
# ambas actividades del pipeline `pl_sales_aw`:
# 
# 1. `Copy job_f0d` → 9 tablas Delta en `Bronze_Landing/Tables`
# 2. `Copy data 1`  → archivo Parquet en `Bronze_Landing/Files/sales/currencyrate`
# 
# #### Consideraciones técnicas
# 
# - **Lakehouse predeterminado:** Bronze_Landing únicamente. Agregar
#   lakehouses innecesarios genera ambigüedad en rutas relativas y riesgo
#   de escritura en la capa incorrecta.
# - **Engine de lectura:** PyArrow (default). `currencyrate` no contiene
#   el tipo `TIME(NANOS,true)` que requiere `fastparquet` — sus campos son
#   `int`, `date`, `string` y `decimal`.
# - **Causa raíz del error:** PostgreSQL permite precisiones `DECIMAL`
#   arbitrarias. Delta Lake impone un máximo de 38 dígitos de precisión
#   total y 18 de escala. Valores fuera de ese rango generan
#   `DeltaInvalidBigDecimalPrecisionScale`. El cast a `double` elimina
#   esa restricción sin pérdida de precisión relevante para análisis.
# - **Rutas ABFS con GUID:** garantizan que el notebook apunte siempre a
#   Bronze_Landing independientemente del entorno o configuraciones futuras.


# MARKDOWN ********************

# ---
# ## 🔧 Configuración del Entorno
# ### Parámetros de Workspace y Rutas de Acceso

# CELL ********************

# =============================================================
# CELDA 1 · Parámetros de workspace
#
# Centraliza GUIDs y rutas base. Modificar solo esta sección
# cuando cambie el workspace, la capa o el dominio de negocio.
#
# WORKSPACE_ID  : GUID del área de trabajo DP700
# BRONZE_ID     : GUID del lakehouse Bronze_Landing
# DOMAIN_FOLDER : Subcarpeta del dominio activo dentro de Files
# BRONZE_BASE   : Ruta ABFS absoluta al dominio activo
#
# NOTA: Silver_Refined no se declara aquí porque este notebook
# opera exclusivamente dentro de Bronze_Landing. Origen y
# destino pertenecen al mismo lakehouse.
# =============================================================

WORKSPACE_ID  = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"
BRONZE_ID     = "f2f5ef99-fa5e-4802-b052-60d3674c2af2"

DOMAIN_FOLDER = "sales"
DOMAIN        = "sales"

BRONZE_BASE = (
    f"abfss://{WORKSPACE_ID}"
    f"@onelake.dfs.fabric.microsoft.com"
    f"/{BRONZE_ID}/Files"
    f"/{DOMAIN_FOLDER}"
)

print("✅ Parámetros cargados")
print(f"   BRONZE_BASE → {BRONZE_BASE}")
print(f"   Dominio     → {DOMAIN_FOLDER}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Imports y Configuración de Spark

# CELL ********************

# =============================================================
# CELDA 2 · Imports y configuración de Spark
#
# datetimeRebaseModeInRead : Corrige fechas históricas de sistemas
#   con calendario juliano (PostgreSQL, Oracle, SQL Server).
# int96RebaseModeInRead    : Aplica la misma corrección al formato
#   INT96 de versiones antiguas de Parquet para timestamps.
# RUN_TS                   : Sello UTC de la ejecución. Propaga
#   trazabilidad a la tabla producida en este notebook.
#
# NOTA: fastparquet no se instala en este notebook porque
# currencyrate no contiene el tipo TIME(NANOS,true). Sus campos
# son int, date, string y decimal — compatibles con PyArrow.
# =============================================================

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DateType, DoubleType
import pandas as pd
from datetime import datetime, timezone

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead",   "CORRECTED")

RUN_TS = datetime.now(timezone.utc).isoformat()

print("✅ Entorno listo")
print(f"   Ejecución UTC → {RUN_TS}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## 📥 Lectura y Normalización de currencyrate
# ### Tratamiento del archivo Parquet con tipos DECIMAL incompatibles con Delta

# CELL ********************

# =============================================================
# CELDA 3 · Lectura del Parquet currencyrate desde Bronze_Landing
#
# El archivo fue depositado por la actividad Copy data 1 del
# pipeline pl_advworks en formato Parquet porque el tipo DECIMAL
# de PostgreSQL genera el error DeltaInvalidBigDecimalPrecisionScale
# al intentar escribirlo directamente como tabla Delta.
#
# PyArrow lee el Parquet sin problemas porque no impone las
# restricciones de precisión que Delta sí impone.
#
# Pandas actúa como puente para inspeccionar el schema antes
# de convertir a Spark y aplicar los casts correctivos.
# =============================================================

print("📥 Leyendo currencyrate desde Bronze_Landing...")

# Pandas lee el Parquet vía ruta ABFS absoluta con GUID
_pdf_cr = pd.read_parquet(f"{BRONZE_BASE}/currencyrate")

print(f"   Schema Pandas (antes de normalización):")
for col, dtype in _pdf_cr.dtypes.items():
    print(f"      {col:<25} {str(dtype)}")

# Elimina columna no requerida en capas superiores
_pdf_cr = _pdf_cr.drop(columns=["modifieddate"], errors="ignore")

print(f"\n   Filas leídas: {len(_pdf_cr)}")
print("✅ Lectura completada")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Conversión a Spark y Normalización de Tipos

# CELL ********************

# =============================================================
# CELDA 4 · Conversión Pandas → Spark y casts correctivos
#
# Los campos averagerate y endofdayrate provienen de PostgreSQL
# como DECIMAL con precisión arbitraria. Delta Lake impone un
# máximo de 38 dígitos de precisión total y 18 de escala.
# Valores fuera de ese rango generan:
#   DeltaInvalidBigDecimalPrecisionScale
#
# La solución es castear esos campos a double, que no impone
# restricciones de precisión y es el tipo estándar para
# valores monetarios y tasas de cambio en modelos analíticos.
#
# _ingestion_ts : Columna de auditoría que registra cuándo
#   fue procesada esta entidad. Permite trazabilidad completa
#   del pipeline origen → Bronze.
# =============================================================

# Pandas → Spark
df_cr = spark.createDataFrame(_pdf_cr)

# Casts explícitos al schema final + columna de auditoría
df_cr = (
    df_cr
    .withColumn("currencyrateid",   F.col("currencyrateid").cast(IntegerType()))
    .withColumn("currencyratedate", F.col("currencyratedate").cast(DateType()))
    .withColumn("fromcurrencycode", F.col("fromcurrencycode").cast(StringType()))
    .withColumn("tocurrencycode",   F.col("tocurrencycode").cast(StringType()))
    .withColumn("averagerate",      F.col("averagerate").cast(DoubleType()))
    .withColumn("endofdayrate",     F.col("endofdayrate").cast(DoubleType()))
    .withColumn("_ingestion_ts",    F.lit(RUN_TS))
)

print("   Schema Spark (después de normalización):")
for field in df_cr.schema.fields:
    print(f"      {field.name:<25} {str(field.dataType)}")

print(f"\n   Filas procesadas: {df_cr.count()}")
print("✅ Normalización completada")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## 💾 Persistencia en Bronze_Landing como Tabla Delta
# ### Origen y destino en el mismo lakehouse — Bronze_Landing

# CELL ********************

# =============================================================
# CELDA 5 · Escritura como tabla Delta en Bronze_Landing
#
# La tabla se escribe bajo el schema bronze_landing para
# mantener consistencia de nomenclatura con las demás entidades
# del dominio sales que el pipeline depositó como Delta.
#
# Al completar esta celda, currencyrate queda en igualdad de
# condiciones con las otras 9 entidades sales — todas accesibles
# como tablas Delta desde el catálogo de Spark.
#
# overwriteSchema : Tolera cambios de schema entre ejecuciones.
# =============================================================

TABLE_NAME = f"bronze_landing.{DOMAIN}_currencyrate"

print(f"💾 Escribiendo tabla Delta → {TABLE_NAME}...")

(
    df_cr.write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .saveAsTable(TABLE_NAME)
)

print(f"✅ Tabla {TABLE_NAME} creada correctamente")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## ✅ Verificación y Resumen de Ejecución

# CELL ********************

# =============================================================
# CELDA 6 · Verificación del resultado y resumen de ejecución
#
# Valida que la tabla existe en el catálogo y contiene datos.
# Muestra las primeras 10 filas para confirmar que los tipos
# de datos fueron normalizados correctamente.
# =============================================================

count = spark.sql(f"SELECT COUNT(*) AS n FROM {TABLE_NAME}").collect()[0]["n"]

print("=" * 55)
print("  RESUMEN DE EJECUCIÓN")
print("=" * 55)
print(f"  {TABLE_NAME:<40} {count:>6} filas")
print("=" * 55)
print(f"  Timestamp : {RUN_TS}")
print(f"  Estado    : ✅ COMPLETADO")
print("=" * 55)

display(spark.sql(f"SELECT * FROM {TABLE_NAME} LIMIT 10"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
