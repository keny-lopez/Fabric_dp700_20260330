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

# # 🏗️ Pipeline Bronze → Silver · Dominio RRHH
# ## Workspace DP700 · AdventureWorks Human Resources
# > **Lakehouse predeterminado:** Silver_Refined  
# > **Origen:** Bronze_Landing · Files/humanresources  
# > **Destino:** Silver_Refined · Tables/dbo

# MARKDOWN ********************

# ---
# ## 📦 Dependencias
# ### Instalación de librerías no incluidas en el entorno base de Fabric

# CELL ********************

# =============================================================
# CELDA 0 · Instalación de dependencias
#
# fastparquet resuelve el tipo INT64 (TIME(NANOS,true)) exportado
# por PostgreSQL, que el parser de schema de Spark rechaza.
# Bug documentado en la comunidad de Fabric sin corrección
# a nivel de motor a abril 2026.
#
# IMPORTANTE: El kernel se reinicia automáticamente tras la
# instalación. Ejecutar todas las celdas siguientes en orden
# después del reinicio.
# =============================================================

%pip install fastparquet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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
# SILVER_ID     : GUID del lakehouse Silver_Refined
# BRONZE_ROOT   : Raíz ABFS de Bronze, agnóstica al dominio
# DOMAIN_FOLDER : Subcarpeta del dominio activo en Bronze
# BRONZE_BASE   : Ruta completa al dominio activo
# =============================================================

# Identificadores únicos obtenidos desde la URL del Fabric Portal
WORKSPACE_ID  = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"
BRONZE_ID     = "f2f5ef99-fa5e-4802-b052-60d3674c2af2"
SILVER_ID     = "c3abadd9-077d-48dd-a313-40970c5853b4"

# Raíz ABFS absoluta de Bronze_Landing.
# El formato onelake.dfs.fabric.microsoft.com resuelve cualquier
# lakehouse del tenant independientemente del predeterminado.
BRONZE_ROOT = (
    f"abfss://{WORKSPACE_ID}"
    f"@onelake.dfs.fabric.microsoft.com"
    f"/{BRONZE_ID}/Files"
)

# Subcarpeta del dominio de negocio activo dentro de Bronze
DOMAIN_FOLDER = "humanresources"

# Ruta completa al dominio activo
BRONZE_BASE = f"{BRONZE_ROOT}/{DOMAIN_FOLDER}"

print("✅ Parámetros cargados")
print(f"   BRONZE_ROOT → {BRONZE_ROOT}")
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
#   Sin esta config, fechas anteriores a 1900 se corrompen
#   silenciosamente al leer Parquet.
# int96RebaseModeInRead    : Aplica la misma corrección al formato
#   INT96 usado por versiones antiguas de Parquet para timestamps.
# RUN_TS                   : Sello UTC de la ejecución. Propaga
#   trazabilidad a todas las tablas producidas en este notebook.
# =============================================================

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
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
# ## 📥 Lectura desde Bronze_Landing
# ### Entidades estándar

# CELL ********************

# =============================================================
# CELDA 3 · Lectura de Parquet desde Bronze_Landing
#
# Aplica a todas las entidades excepto shift,
# que requiere tratamiento especial en Celda 4.
#
# mergeSchema    : Tolera evolución de schema entre ejecuciones.
#   Si el origen agrega columnas, el proceso no falla.
# _ingestion_ts  : Registra cuándo ingresó cada fila al pipeline.
# _source_entity : Registra el nombre de la entidad de origen.
# =============================================================

def read_bronze(entity: str):
    """
    Lee una entidad Parquet desde Bronze_Landing vía ruta ABFS absoluta.
    Registra timestamp de ingesta y nombre de entidad como columnas
    de auditoría en cada fila del DataFrame resultante.

    Args:
        entity: Nombre de la subcarpeta Parquet dentro de BRONZE_BASE.

    Returns:
        DataFrame de Spark con columnas de auditoría añadidas.
    """
    path = f"{BRONZE_BASE}/{entity}"

    df = (
        spark.read
             .format("parquet")
             .option("mergeSchema", "true")
             .load(path)
             .withColumn("_ingestion_ts",  F.lit(RUN_TS))
             .withColumn("_source_entity", F.lit(entity))
    )

    print(f"   ✔ {entity:<35} {df.count():>6} filas")
    return df


print("📥 Leyendo entidades desde Bronze_Landing...")

df_department          = read_bronze("department")
df_employeedepthistory = read_bronze("employeedepartmenthistory")
df_employeepayhistory  = read_bronze("employeepayhistory")
df_jobcandidate        = read_bronze("jobcandidate")

print("✅ Lectura completada")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Tratamiento Especial: Entidad Shift

# CELL ********************

# =============================================================
# CELDA 4 · Lectura y normalización de la entidad shift
#
# El tipo INT64 (TIME(NANOS,true)) exportado por PostgreSQL es
# rechazado por el parser de schema de Spark. Bug documentado
# en la comunidad de Fabric sin corrección a nivel de motor
# a abril 2026.
#
# fastparquet interpreta TIME(NANOS,true) nativamente y actúa
# como capa de conversión antes de entregar el DataFrame a Spark.
#
# Flujo:
#   1. fastparquet lee el Parquet desde ABFS (ruta con GUID)
#   2. Pandas normaliza campos TIME a string "HH:MM:SS"
#   3. Spark recibe el DataFrame ya con schema compatible
#   4. Casts explícitos garantizan el schema final esperado
# =============================================================

print("⚙️  Procesando entidad shift...")

# Paso 1: fastparquet lee desde ABFS tolerando TIME(NANOS,true)
_pdf_shift = pd.read_parquet(
    f"{BRONZE_BASE}/shift",
    engine="fastparquet"
)

print(f"   Schema Pandas (antes): { {c: str(t) for c, t in _pdf_shift.dtypes.items()} }")

# Paso 2a: Elimina columna no requerida en capas superiores
_pdf_shift = _pdf_shift.drop(columns=["modifieddate"], errors="ignore")

# Paso 2b: Convierte campos TIME a string para compatibilidad con Spark
for _col in ["starttime", "endtime"]:
    if _col in _pdf_shift.columns:
        _pdf_shift[_col] = _pdf_shift[_col].astype(str)

# Paso 3: Pandas → Spark
df_shift = spark.createDataFrame(_pdf_shift)

# Paso 4: Casts explícitos al schema final + columnas de auditoría
df_shift = (
    df_shift
    .withColumn("shiftid",        F.col("shiftid").cast(IntegerType()))
    .withColumn("name",           F.col("name").cast(StringType()))
    .withColumn("starttime",      F.col("starttime").cast(StringType()))
    .withColumn("endtime",        F.col("endtime").cast(StringType()))
    .withColumn("_ingestion_ts",  F.lit(RUN_TS))
    .withColumn("_source_entity", F.lit("shift"))
)

print(f"   ✔ shift                              {df_shift.count():>6} filas")
print("✅ Entidad shift procesada")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## 💾 Persistencia en Bronze_Landing
# ### Materialización como Tablas Delta en el Catálogo de Spark

# CELL ********************

# =============================================================
# CELDA 5 · Materialización de entidades en Bronze_Landing
#
# Convierte los archivos Parquet no administrados (Files) en
# tablas Delta registradas en el catálogo de Spark bajo el
# schema bronze_landing. Habilita consultas SQL directas
# desde cualquier notebook o pipeline que referencie Bronze.
#
# DOMAIN          : Prefijo de dominio aplicado a nombres de tabla.
# overwriteSchema : Tolera cambios de schema entre ejecuciones.
#
# Las columnas de auditoría (_ingestion_ts, _source_entity)
# se excluyen de Bronze — su destino es Silver.
# =============================================================

DOMAIN = "hr"

def save_to_bronze(df, entity_name: str):
    """
    Persiste un DataFrame como tabla Delta en bronze_landing.

    Args:
        df:           DataFrame de Spark con columnas de auditoría.
        entity_name:  Nombre de la entidad (sufijo del nombre de tabla).
    """
    table = f"bronze_landing.{DOMAIN}_{entity_name}"

    (
        df.drop("_ingestion_ts", "_source_entity")
          .write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(table)
    )
    print(f"   ✔ {table}")


print("💾 Persistiendo tablas Delta en Bronze_Landing...")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze_landing")

save_to_bronze(df_department,          "department")
save_to_bronze(df_employeedepthistory, "employeedepartmenthistory")
save_to_bronze(df_employeepayhistory,  "employeepayhistory")
save_to_bronze(df_jobcandidate,        "jobcandidate")
save_to_bronze(df_shift,               "shift")

print("✅ Bronze materializado en catálogo")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## 🔄 Transformaciones Silver
# ### Construcción del Modelo Dimensional

# CELL ********************

# =============================================================
# CELDA 6 · Construcción del modelo dimensional en Silver
#
# Produce 4 objetos analíticos desde las tablas Delta de Bronze:
#   · ft_costos_rrhh_dia_turno_depto_business_entity
#       Tabla de hechos con granularidad diaria por empleado,
#       departamento y turno. Calcula costo_dia según
#       la frecuencia de pago del empleado.
#   · dim_trabajador   → fechas de inicio y término por empleado
#   · dim_departamento → catálogo de departamentos y grupos
#   · dim_turno        → catálogo de turnos con horarios
#
# Todas las consultas referencian bronze_landing para mantener
# la separación de capas. Silver no lee desde Files directamente.
# =============================================================

print("🔄 Construyendo modelo dimensional Silver...")

# ── Tabla de Hechos ───────────────────────────────────────────
df_ft_costos = spark.sql(f"""
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
        END AS costo_dia,
        '{RUN_TS}' AS _ingestion_ts
    FROM bronze_landing.hr_employeedepartmenthistory edh
    LEFT JOIN bronze_landing.hr_employeepayhistory eph
        ON edh.businessentityid = eph.businessentityid
    ORDER BY edh.businessentityid, eph.ratechangedate
""")

# ── Dimensión Trabajador ──────────────────────────────────────
df_dim_trabajador = spark.sql(f"""
    SELECT
        businessentityid,
        MIN(startdate) AS fechainicio,
        MAX(enddate)   AS fechatermino,
        '{RUN_TS}'     AS _ingestion_ts
    FROM bronze_landing.hr_employeedepartmenthistory
    GROUP BY businessentityid
""")

# ── Dimensión Departamento ────────────────────────────────────
df_dim_departamento = spark.sql(f"""
    SELECT
        departmentid,
        name       AS departmentname,
        groupname,
        '{RUN_TS}' AS _ingestion_ts
    FROM bronze_landing.hr_department
""")

# ── Dimensión Turno ───────────────────────────────────────────
df_dim_turno = spark.sql(f"""
    SELECT
        shiftid,
        name      AS shiftname,
        starttime,
        endtime,
        '{RUN_TS}' AS _ingestion_ts
    FROM bronze_landing.hr_shift
""")

print(f"   ✔ ft_costos_rrhh_dia_turno_depto_business_entity → {df_ft_costos.count():>6} filas")
print(f"   ✔ dim_trabajador                                 → {df_dim_trabajador.count():>6} filas")
print(f"   ✔ dim_departamento                               → {df_dim_departamento.count():>6} filas")
print(f"   ✔ dim_turno                                      → {df_dim_turno.count():>6} filas")
print("✅ Transformaciones completadas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Escritura en Silver_Refined como Tablas Delta

# CELL ********************

# =============================================================
# CELDA 7 · Persistencia del modelo dimensional en Silver_Refined
#
# Las tablas se escriben sin prefijo de schema para que Fabric
# las registre bajo dbo, el schema predeterminado del lakehouse.
# Esto las hace visibles en el selector de modelo semántico
# y las coloca directamente bajo Tables/dbo sin subcarpetas.
#
# Silver_Refined debe ser el lakehouse predeterminado del notebook.
# =============================================================

def save_to_silver(df, table_name: str):
    """
    Persiste un DataFrame como tabla Delta en Silver_Refined.
    Registra la tabla bajo dbo para visibilidad en modelo semántico.

    Args:
        df:           DataFrame de Spark con columnas de auditoría.
        table_name:   Nombre de la tabla destino.
    """
    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(table_name)
    )
    print(f"   ✔ {table_name}")


print("💾 Escribiendo tablas Delta en Silver_Refined...")

save_to_silver(df_ft_costos,        "ft_costos_rrhh_dia_turno_depto_business_entity")
save_to_silver(df_dim_trabajador,   "dim_trabajador")
save_to_silver(df_dim_departamento, "dim_departamento")
save_to_silver(df_dim_turno,        "dim_turno")

print("✅ Silver_Refined actualizado")

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
# CELDA 8 · Verificación del resultado y resumen de ejecución
#
# Valida que cada tabla Silver existe y contiene datos.
# Los nombres de tabla se referencian sin prefijo de schema
# porque residen bajo dbo, el schema predeterminado del lakehouse.
#
# En producción este bloque se reemplaza por un framework
# de calidad de datos (Great Expectations, Fabric DQ Rules).
# =============================================================

TABLAS_SILVER = [
    ("ft_costos_rrhh_dia_turno_depto_business_entity", "Tabla de Hechos"),
    ("dim_trabajador",                                  "Dimensión"),
    ("dim_departamento",                                "Dimensión"),
    ("dim_turno",                                       "Dimensión"),
]

print("=" * 65)
print("  RESUMEN DE EJECUCIÓN")
print("=" * 65)

for tabla, tipo in TABLAS_SILVER:
    count = spark.sql(f"SELECT COUNT(*) AS n FROM {tabla}").collect()[0]["n"]
    print(f"  {tabla:<50} {count:>5} filas  [{tipo}]")

print("=" * 65)
print(f"  Timestamp : {RUN_TS}")
print(f"  Estado    : ✅ COMPLETADO")
print("=" * 65)

display(spark.sql("SELECT * FROM ft_costos_rrhh_dia_turno_depto_business_entity LIMIT 10"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
