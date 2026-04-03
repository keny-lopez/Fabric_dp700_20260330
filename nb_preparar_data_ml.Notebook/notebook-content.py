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

# # 🏗️ Pipeline Silver → Gold · Preparación de Datos para ML
# ## Workspace DP700 · AdventureWorks RRHH
# ---
# ### Contexto y propósito
# 
# Este notebook mueve datos desde **Silver_Refined** hacia **Gold_Trusted**,
# construyendo el dataset de entrenamiento para modelos de ML de analítica
# prescriptiva sobre costos de RRHH.
# 
# #### Etapa previa requerida
# 
# Antes de ejecutar este notebook deben existir en Silver_Refined:
# 
# - `ft_costos_rrhh_dia_turno_depto_business_entity`
# - `dim_departamento`
# 
# Ambas tablas son producidas por `nb_analisis_rrhh`.
# 
# #### Convención de lakehouses en este notebook
# 
# | Lakehouse | Rol | GUID |
# |---|---|---|
# | `Gold_Trusted` | Predeterminado — destino de escritura | `4e079e43-45c5-42e9-bc33-558028f6866d` |
# | `Silver_Refined` | Secundario — origen de lectura | `c3abadd9-077d-48dd-a313-40970c5853b4` |
# 
# #### Consideraciones técnicas
# 
# - Las tablas de Silver se referencian con ruta ABFS absoluta con GUID
#   para garantizar que el notebook apunte siempre a Silver_Refined
#   independientemente del lakehouse predeterminado.
# - La tabla destino en Gold se escribe sin prefijo de schema para que
#   Fabric la registre bajo `dbo` — visible en el selector de modelo
#   semántico y en Direct Lake.
# - `%%sql` no resuelve lakehouses secundarios — todas las consultas
#   usan `spark.sql()` para garantizar compatibilidad.


# MARKDOWN ********************

# ---
# ## 🔧 Configuración del Entorno
# ### Parámetros de Workspace y Rutas de Acceso

# CELL ********************

# =============================================================
# CELDA 1 · Parámetros de workspace
#
# WORKSPACE_ID : GUID del área de trabajo DP700
# SILVER_ID    : GUID del lakehouse Silver_Refined (origen)
# GOLD_ID      : GUID del lakehouse Gold_Trusted (destino)
#
# Las rutas ABFS absolutas con GUID garantizan que el notebook
# resuelva siempre el lakehouse correcto independientemente
# del predeterminado configurado en el entorno.
# =============================================================

WORKSPACE_ID = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"
SILVER_ID    = "c3abadd9-077d-48dd-a313-40970c5853b4"
GOLD_ID      = "4e079e43-45c5-42e9-bc33-558028f6866d"

# Raíz ABFS de Silver_Refined — origen de lectura
SILVER_ROOT = (
    f"abfss://{WORKSPACE_ID}"
    f"@onelake.dfs.fabric.microsoft.com"
    f"/{SILVER_ID}/Tables"
)

print("✅ Parámetros cargados")
print(f"   SILVER_ROOT → {SILVER_ROOT}")

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
# RUN_TS : Sello UTC de la ejecución. Propaga trazabilidad
#   a la tabla Gold producida en este notebook.
# =============================================================

from pyspark.sql import functions as F
from datetime import datetime, timezone

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
# ## 📥 Lectura desde Silver_Refined
# ### Verificación de tablas de origen

# CELL ********************

# =============================================================
# CELDA 3 · Lectura de tablas desde Silver_Refined
#
# Las tablas residen bajo el schema dbo de Silver_Refined.
# La ruta física correcta en OneLake incluye el subdirectorio
# dbo entre Tables y el nombre de la tabla.
#
# Se registran como vistas temporales de Spark para poder
# referenciarlas en spark.sql() sin prefijo de schema,
# evitando dependencias del catálogo del lakehouse secundario.
# =============================================================

print("📥 Leyendo tablas desde Silver_Refined...")

# Tabla de hechos
df_ft_costos = (
    spark.read
         .format("delta")
         .load(f"{SILVER_ROOT}/dbo/ft_costos_rrhh_dia_turno_depto_business_entity")
)
df_ft_costos.createOrReplaceTempView("ft_costos")

# Dimensión departamento
df_dim_depto = (
    spark.read
         .format("delta")
         .load(f"{SILVER_ROOT}/dbo/dim_departamento")
)
df_dim_depto.createOrReplaceTempView("dim_departamento")

print(f"   ✔ ft_costos_rrhh_dia_turno_depto_business_entity → {df_ft_costos.count():>6} filas")
print(f"   ✔ dim_departamento                               → {df_dim_depto.count():>6} filas")
print("✅ Lectura completada")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## 🔄 Transformación · Dataset de Entrenamiento ML
# ### Construcción del dataset con lógica de negocio RRHH

# CELL ********************

# =============================================================
# CELDA 4 · Construcción del dataset de entrenamiento
#
# Consolida la tabla de hechos con la dimensión departamento
# para producir un dataset enriquecido listo para entrenar
# modelos de ML de analítica prescriptiva sobre costos de RRHH.
#
# Las vistas temporales registradas en Celda 3 permiten
# usar spark.sql() sin referenciar el lakehouse secundario.
#
# Columnas producidas:
#   idtrabajador : Identificador único del empleado
#   idturno      : Identificador del turno asignado
#   fechahora    : Fecha del cambio de tasa (casteada a date)
#   tasa         : Tasa de pago del empleado
#   frecpago     : Frecuencia de pago (1=mensual, 2=quincenal)
#   costo_dia    : Costo diario calculado en Bronze→Silver
#   departamento : Nombre del departamento
#   grupo        : Grupo funcional del departamento
#   _ingestion_ts: Sello de auditoría UTC de esta ejecución
# =============================================================

df_entrenamiento = spark.sql(f"""
    SELECT
        A.businessentityid              AS idtrabajador,
        A.shiftid                       AS idturno,
        CAST(A.ratechangedate AS date)  AS fechahora,
        A.rate                          AS tasa,
        A.payfrequency                  AS frecpago,
        A.costo_dia,
        B.departmentname                AS departamento,
        B.groupname                     AS grupo,
        '{RUN_TS}'                      AS _ingestion_ts
    FROM ft_costos A
    INNER JOIN dim_departamento B
        ON B.departmentid = A.departmentid
""")

print(f"   ✔ Dataset de entrenamiento → {df_entrenamiento.count():>6} filas")
display(df_entrenamiento)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## 💾 Persistencia en Gold_Trusted
# ### Escritura del dataset de entrenamiento como tabla Delta

# CELL ********************

# =============================================================
# CELDA 5 · Escritura del dataset de entrenamiento en Gold_Trusted
#
# La tabla se escribe sin prefijo de schema para que Fabric
# la registre bajo dbo — el schema predeterminado de Gold_Trusted.
# Esto la hace visible en el selector de modelo semántico
# y en Direct Lake sin configuración adicional.
#
# Gold_Trusted debe ser el lakehouse predeterminado del notebook.
# =============================================================

TABLE_NAME = "train_tasatrabajo"

print(f"💾 Escribiendo tabla Delta → {TABLE_NAME} en Gold_Trusted...")

(
    df_entrenamiento.write
                    .format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .saveAsTable(TABLE_NAME)
)

print(f"✅ Tabla {TABLE_NAME} escrita en Gold_Trusted")

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
# Valida que la tabla existe en Gold_Trusted y contiene datos.
# Muestra las primeras 10 filas para confirmar que el JOIN
# y los casts se aplicaron correctamente.
# =============================================================

count = spark.sql(f"SELECT COUNT(*) AS n FROM {TABLE_NAME}").collect()[0]["n"]

print("=" * 60)
print("  RESUMEN DE EJECUCIÓN")
print("=" * 60)
print(f"  {TABLE_NAME:<45} {count:>6} filas")
print("=" * 60)
print(f"  Timestamp : {RUN_TS}")
print(f"  Estado    : ✅ COMPLETADO")
print("=" * 60)

display(spark.sql(f"SELECT * FROM {TABLE_NAME} LIMIT 10"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
