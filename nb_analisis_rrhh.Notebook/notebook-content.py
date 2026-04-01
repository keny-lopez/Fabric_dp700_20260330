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

# CELL ********************

%pip install fastparquet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🔧 Configuración del Entorno
# ### Parámetros de Workspace y Rutas de Acceso

# CELL ********************

# =============================================================
# CELDA 1 · Parámetros de workspace
# Centraliza GUIDs y rutas base. Modificar solo esta sección
# cuando cambie el workspace, la capa o el dominio de negocio.
# =============================================================

# Identificadores únicos del workspace y lakehouses (Fabric Portal → URL)
WORKSPACE_ID = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"
BRONZE_ID    = "f2f5ef99-fa5e-4802-b052-60d3674c2af2"
SILVER_ID    = "c3abadd9-077d-48dd-a313-40970c5853b4"

# Raíz ABFS del lakehouse Bronze. Agnóstica al dominio de negocio.
BRONZE_ROOT = (
    f"abfss://{WORKSPACE_ID}"
    f"@onelake.dfs.fabric.microsoft.com"
    f"/{BRONZE_ID}/Files"
)

# Subcarpeta del dominio de negocio activo dentro de Bronze
DOMAIN_FOLDER = "humanresources"

# Ruta completa al dominio activo dentro de Bronze
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
# =============================================================

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
import pandas as pd
from datetime import datetime, timezone

# Corrige la interpretación de fechas históricas provenientes
# de sistemas con calendario juliano (PostgreSQL, Oracle, SQL Server).
# Sin esta configuración, fechas anteriores a 1900 se corrompen
# silenciosamente al leer Parquet.
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead",   "CORRECTED")

# Sello de tiempo UTC de la ejecución. Propaga trazabilidad
# a todas las tablas producidas en este notebook.
RUN_TS = datetime.now(timezone.utc).isoformat()

print("✅ Entorno listo")
print(f"   Ejecución UTC → {RUN_TS}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Lectura de Entidades desde Bronze_Landing

# CELL ********************

# =============================================================
# CELDA 3 · Lectura de Parquet desde Bronze_Landing
# Aplica a todas las entidades excepto shift,
# que requiere tratamiento especial en Celda 4.
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
             .withColumn("_ingestion_ts",    F.lit(RUN_TS))
             .withColumn("_source_entity",   F.lit(entity))
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
# en la comunidad de Fabric sin corrección a nivel de motor.
# fastparquet interpreta TIME(NANOS,true) nativamente y actúa
# como capa de conversión antes de entregar el DataFrame a Spark.
# =============================================================

print("⚙️  Procesando entidad shift...")

# fastparquet lee desde la ruta ABFS absoluta con GUID.
# El engine fastparquet resuelve TIME(NANOS,true) sin error.
_pdf_shift = pd.read_parquet(
    f"{BRONZE_BASE}/shift",
    engine="fastparquet"
)

print(f"   Schema Pandas (antes): { {c: str(t) for c, t in _pdf_shift.dtypes.items()} }")

# Elimina columna no requerida en capas superiores
_pdf_shift = _pdf_shift.drop(columns=["modifieddate"], errors="ignore")

# Convierte campos TIME a string "HH:MM:SS"
for _col in ["starttime", "endtime"]:
    if _col in _pdf_shift.columns:
        _pdf_shift[_col] = _pdf_shift[_col].astype(str)

# Pandas → Spark
df_shift = spark.createDataFrame(_pdf_shift)

# Casts explícitos al schema final + columnas de auditoría
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

# ### Persistencia en Bronze_Landing como Tablas Delta

# CELL ********************

# =============================================================
# CELDA 5 · Materialización de entidades en Bronze_Landing
#
# Convierte los archivos Parquet no administrados (Files) en
# tablas Delta registradas en el catálogo de Spark bajo el
# schema bronze_landing. Esto habilita consultas SQL directas
# desde cualquier notebook o pipeline que referencie Bronze.
#
# Las columnas de auditoría (_ingestion_ts, _source_entity)
# se excluyen de Bronze — su destino es Silver.
# =============================================================

# Prefijo aplicado a todos los nombres de tabla en Bronze
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

# ### Transformaciones Silver: Modelo Dimensional

# CELL ********************

# =============================================================
# CELDA 6 · Construcción del modelo dimensional en Silver
#
# Produce 4 objetos analíticos desde las tablas Delta de Bronze:
#   · ft_costos_rrhh_dia  → tabla de hechos (granularidad diaria)
#   · dim_trabajador      → dimensión de empleados
#   · dim_departamento    → dimensión de departamentos
#   · dim_turno           → dimensión de turnos
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

print(f"   ✔ ft_costos_rrhh_dia   → {df_ft_costos.count():>6} filas")
print(f"   ✔ dim_trabajador       → {df_dim_trabajador.count():>6} filas")
print(f"   ✔ dim_departamento     → {df_dim_departamento.count():>6} filas")
print(f"   ✔ dim_turno            → {df_dim_turno.count():>6} filas")
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
# Silver_Refined debe ser el lakehouse predeterminado del notebook
# para que saveAsTable resuelva el schema silver_refined
# correctamente sin requerir rutas absolutas.
# =============================================================

def save_to_silver(df, table_name: str):
    """
    Persiste un DataFrame como tabla Delta en silver_refined.

    Args:
        df:           DataFrame de Spark con columnas de auditoría.
        table_name:   Nombre de la tabla destino en silver_refined.
    """
    full_name = f"silver_refined.{table_name}"

    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(full_name)
    )
    print(f"   ✔ {full_name}")


print("💾 Escribiendo tablas Delta en Silver_Refined...")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver_refined")

save_to_silver(df_ft_costos,        "ft_costos_rrhh_dia")
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

# ### Verificación y Resumen de Ejecución

# CELL ********************

# =============================================================
# CELDA 8 · Verificación del resultado y resumen de ejecución
#
# Valida que cada tabla Silver existe y contiene datos.
# En producción este bloque se reemplaza por un framework
# de calidad de datos (Great Expectations, Fabric DQ Rules).
# =============================================================

TABLAS_SILVER = [
    ("silver_refined.ft_costos_rrhh_dia",  "Tabla de Hechos"),
    ("silver_refined.dim_trabajador",       "Dimensión"),
    ("silver_refined.dim_departamento",     "Dimensión"),
    ("silver_refined.dim_turno",            "Dimensión"),
]

print("=" * 55)
print("  RESUMEN DE EJECUCIÓN")
print("=" * 55)

for tabla, tipo in TABLAS_SILVER:
    count = spark.sql(f"SELECT COUNT(*) AS n FROM {tabla}").collect()[0]["n"]
    nombre = tabla.split(".")[1]
    print(f"  {nombre:<38} {count:>5} filas  [{tipo}]")

print("=" * 55)
print(f"  Timestamp : {RUN_TS}")
print(f"  Estado    : ✅ COMPLETADO")
print("=" * 55)

# Vista previa de la tabla de hechos
display(spark.sql("SELECT * FROM silver_refined.ft_costos_rrhh_dia LIMIT 10"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
