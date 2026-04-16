# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Detectando cambios en dimensiones con Hashing SHA-256
# ### Microsoft Fabric · PySpark · Modern Data Warehouse
# 
# <table style="border:none; background:transparent;"><tr>
# <td style="border:none; padding:0 12px 0 0;"><img src="https://flagcdn.com/48x36/pa.png" width="48" height="36"> Panamá</td>
# <td style="border:none; padding:0 12px;">·</td>
# <td style="border:none; padding:0 12px;"><img src="https://flagcdn.com/48x36/cr.png" width="48" height="36"> Costa Rica</td>
# <td style="border:none; padding:0 12px;">·</td>
# <td style="border:none; padding:0 12px;"><img src="https://flagcdn.com/48x36/hn.png" width="48" height="36"> Honduras</td>
# </tr></table>
# 
# Comparar columna por columna es un método frágil para detectar cambios
# en una dimensión SCD Tipo 2. En este Notebook se presenta una solución
# usando una huella digital (hash) por registro que funciona sin importar
# cuántas columnas tenga la tabla.
# 
# **Este notebook:**
# - Carga la tabla original de clientes (V1)
# - Simula cambios en tres registros (V2)
# - Detecta cambios sin hashing — identifica el problema
# - Detecta cambios con Hashing SHA-256 — aplica la solución


# CELL ********************

# ============================================================
# Carga de datos — Tabla original de clientes
# ============================================================
# Objetivo:
#   Crear la tabla base con 10 clientes ficticios distribuidos
#   entre Panamá, Costa Rica y Honduras.
#   Esta es la "fotografía inicial" de nuestra dimensión,
#   antes de que ocurra cualquier cambio en los datos.
#
# Lo que hace este bloque:
#   1. Define la estructura (schema) de la tabla:
#      cliente_id, nombre, email, teléfono y ciudad.
#   2. Carga los 10 registros en un DataFrame de Spark.
#   3. Registra la tabla como vista temporal para consultarla
#      con SQL más adelante si fuera necesario.
#   4. Muestra el resultado con display().
# ============================================================

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

datos_v1 = [
    (1,  "Andrés Fábrega",   "andres.fabrega@email.com",  "+507-6601-1111", "Ciudad de Panamá"),
    (2,  "Valeria Mora",     "valeria.mora@email.com",    "+506-8802-2222", "San José"),
    (3,  "Diego Martínez",   "diego.martinez@email.com",  "+507-6603-3333", "Colón"),
    (4,  "Sofía Jiménez",    "sofia.jimenez@email.com",   "+506-8804-4444", "Alajuela"),
    (5,  "Carlos Mejía",     "carlos.mejia@email.com",    "+504-9905-5555", "Tegucigalpa"),
    (6,  "María Reyes",      "maria.reyes@email.com",     "+504-9906-6666", "San Pedro Sula"),
    (7,  "Luis Quesada",     "luis.quesada@email.com",    "+506-8807-7777", "Heredia"),
    (8,  "Gabriela Ramos",   "gabriela.ramos@email.com",  "+507-6608-8888", "David"),
    (9,  "Pablo Solís",      "pablo.solis@email.com",     "+506-8809-9999", "Cartago"),
    (10, "Natalia Herrera",  "natalia.herrera@email.com", "+507-6610-1010", "Chitré"),
]

schema = StructType([
    StructField("cliente_id", IntegerType(), False),  # llave primaria, no puede ser nulo
    StructField("nombre",     StringType(),  True),
    StructField("email",      StringType(),  True),
    StructField("telefono",   StringType(),  True),
    StructField("ciudad",     StringType(),  True),
])

# Crear el DataFrame a partir de los datos y el schema
df_v1 = spark.createDataFrame(datos_v1, schema)

# Registrar como vista temporal para poder usarla con SQL si se necesita
df_v1.createOrReplaceTempView("dim_clientes_v1")

# Mostrar la tabla de forma interactiva
display(df_v1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Simulación de cambios — Tabla de clientes actualizada
# ============================================================
# Objetivo:
#   Representar una actualización real de la dimensión donde
#   algunos clientes modificaron su información de contacto.
#   Este escenario ocurre constantemente en producción.
#
# Cambios aplicados deliberadamente:
#   - Diego Martínez (ID 3): cambió su email
#   - Luis Quesada   (ID 7): cambió su teléfono
#   - Pablo Solís    (ID 9): cambió su ciudad
#
# El reto: ¿cómo detectar estos 3 cambios de forma
# eficiente sin comparar columna por columna?
# ============================================================

datos_v2 = [
    (1,  "Andrés Fábrega",   "andres.fabrega@email.com",      "+507-6601-1111", "Ciudad de Panamá"),
    (2,  "Valeria Mora",     "valeria.mora@email.com",        "+506-8802-2222", "San José"),
    (3,  "Diego Martínez",   "diego.martinez.nuevo@email.com","+507-6603-3333", "Colón"),            # email cambió
    (4,  "Sofía Jiménez",    "sofia.jimenez@email.com",       "+506-8804-4444", "Alajuela"),
    (5,  "Carlos Mejía",     "carlos.mejia@email.com",        "+504-9905-5555", "Tegucigalpa"),
    (6,  "María Reyes",      "maria.reyes@email.com",         "+504-9906-6666", "San Pedro Sula"),
    (7,  "Luis Quesada",     "luis.quesada@email.com",        "+506-8820-0000", "Heredia"),          # teléfono cambió
    (8,  "Gabriela Ramos",   "gabriela.ramos@email.com",      "+507-6608-8888", "David"),
    (9,  "Pablo Solís",      "pablo.solis@email.com",         "+506-8809-9999", "San José"),         # ciudad cambió
    (10, "Natalia Herrera",  "natalia.herrera@email.com",     "+507-6610-1010", "Chitré"),
]

df_v2 = spark.createDataFrame(datos_v2, schema)

# Registrar como vista temporal
df_v2.createOrReplaceTempView("dim_clientes_v2")

# Mostrar la tabla actualizada
display(df_v2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Identificación de cambios — Comparación directa
# ============================================================
# Objetivo:
#   Mostrar visualmente qué registros cambiaron entre la tabla
#   original y la tabla actualizada, y exactamente qué campo
#   fue modificado en cada caso.
#
# Lo que hace este bloque:
#   1. Une ambas tablas por cliente_id.
#   2. Filtra únicamente los registros donde al menos un campo
#      es diferente entre V1 y V2.
#   3. Muestra el valor ANTES y DESPUES de cada campo
#      para que el cambio sea evidente de un vistazo.
# ============================================================

from pyspark.sql.functions import col

df_diff = df_v1.alias("v1").join(df_v2.alias("v2"), on="cliente_id") \
    .filter(
        (col("v1.email")    != col("v2.email"))    |
        (col("v1.telefono") != col("v2.telefono")) |
        (col("v1.ciudad")   != col("v2.ciudad"))
    ) \
    .select(
        col("cliente_id"),
        col("v1.nombre"),
        col("v1.email").alias("email_ANTES"),
        col("v2.email").alias("email_DESPUES"),
        col("v1.telefono").alias("tel_ANTES"),
        col("v2.telefono").alias("tel_DESPUES"),
        col("v1.ciudad").alias("ciudad_ANTES"),
        col("v2.ciudad").alias("ciudad_DESPUES"),
    )

# Mostrar solo los registros que tuvieron cambios
display(df_diff)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# El problema — Detección de cambios sin hashing
# ============================================================
# Objetivo:
#   Demostrar por qué comparar columna por columna es un
#   método frágil y difícil de mantener en producción.
#
# Lo que hace este bloque:
#   1. Compara cada columna individualmente entre V1 y V2.
#   2. Etiqueta cada campo como "CAMBIÓ" o "igual".
#   3. Muestra el resultado por cliente.
#
# El problema visible:
#   Este código tiene 3 condiciones para 3 columnas.
#   En una dimensión real con 50 o 100 columnas,
#   tendrías que escribir y mantener 100 condiciones.
#   Un campo nuevo en la tabla = reescribir el código.
# ============================================================

from pyspark.sql.functions import when, lit

df_comparacion = df_v1.alias("v1").join(df_v2.alias("v2"), on="cliente_id") \
    .withColumn("cambio_email",
        when(col("v1.email")    != col("v2.email"),    lit("🟥CAMBIÓ")).otherwise(lit("✅IGUAL"))) \
    .withColumn("cambio_telefono",
        when(col("v1.telefono") != col("v2.telefono"), lit("🟥CAMBIÓ")).otherwise(lit("✅IGUAL"))) \
    .withColumn("cambio_ciudad",
        when(col("v1.ciudad")   != col("v2.ciudad"),   lit("🟥CAMBIÓ")).otherwise(lit("✅IGUAL"))) \
    .select(
        col("cliente_id"),
        col("v1.nombre"),
        col("cambio_email"),
        col("cambio_telefono"),
        col("cambio_ciudad"),
    )

# Mostrar el resultado — nota cuántas condiciones se necesitaron
# solo para 3 columnas
display(df_comparacion)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# La solución — Detección de cambios con Hashing SHA-256
# ============================================================
# Objetivo:
#   Reemplazar las 100 condiciones del método anterior por
#   una sola comparación usando una huella digital única
#   por registro, generada con el algoritmo SHA-256.
#
# Lo que hace este bloque:
#   1. Concatena todos los campos de cada registro en
#      un solo texto separado por "|".
#   2. Aplica SHA-256 a ese texto para producir una huella
#      digital de 64 caracteres (hash_registro).
#      Mostramos solo los primeros 20 para legibilidad.
#   3. Compara el hash de V1 contra el hash de V2.
#      Si son distintos → el registro cambió.
#      Si son iguales  → no hubo ningún cambio.
#
# La ventaja:
#   Si mañana la tabla tiene 200 columnas, este código
#   no cambia. Solo se actualiza la lista COLUMNAS.
# ============================================================

from pyspark.sql.functions import sha2, concat_ws, substring, when, lit, col

COLUMNAS  = ["nombre", "email", "telefono", "ciudad"]
SEPARADOR = "|"

# Generar la huella digital para cada registro en V1
df_v1_hash = df_v1.withColumn(
    "hash_registro",
    sha2(concat_ws(SEPARADOR, *[col(c) for c in COLUMNAS]), 256)
)

# Generar la huella digital para cada registro en V2
df_v2_hash = df_v2.withColumn(
    "hash_registro",
    sha2(concat_ws(SEPARADOR, *[col(c) for c in COLUMNAS]), 256)
)

# Comparar hashes y recortar a 20 caracteres para legibilidad
df_resultado = df_v1_hash.alias("v1").join(
    df_v2_hash.alias("v2"), on="cliente_id"
) \
.withColumn(
    "estado",
    when(col("v1.hash_registro") != col("v2.hash_registro"), lit("🟥CAMBIÓ"))
    .otherwise(lit("✅SIN CAMBIOS"))
) \
.withColumn("hash_v1", substring(col("v1.hash_registro"), 1, 20)) \
.withColumn("hash_v2", substring(col("v2.hash_registro"), 1, 20)) \
.select(
    col("cliente_id"),
    col("v1.nombre"),
    col("hash_v1"),
    col("hash_v2"),
    col("estado")
)

# Mostrar el resultado ordenado por ID
display(df_resultado.orderBy("cliente_id"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# El momento "Eureka" — Un cambio mínimo, un hash completamente
# distinto
# ============================================================
# Objetivo:
#   Demostrar una propiedad fundamental del algoritmo SHA-256:
#   un cambio de un solo carácter en el dato original produce
#   un hash completamente diferente.
#
# Lo que hace este bloque:
#   1. Aísla el registro de Diego Martínez (ID 3) en ambas
#      versiones de la tabla.
#   2. Muestra su email ANTES y DESPUES del cambio.
#   3. Muestra el hash completo de cada versión para que
#      se vea que son totalmente distintos aunque el email
#      solo cambió unos pocos caracteres.
#
# Por qué importa:
#   Esto garantiza que ningún cambio pase desapercibido,
#   sin importar qué tan pequeño sea.
# ============================================================

df_diego = df_v1_hash.alias("v1").join(
    df_v2_hash.alias("v2"), on="cliente_id"
) \
.filter(col("cliente_id") == 3) \
.select(
    col("v1.nombre"),
    col("v1.email").alias("email_V1"),
    col("v2.email").alias("email_V2"),
    col("v1.hash_registro").alias("hash_V1"),
    col("v2.hash_registro").alias("hash_V2"),
)

# Mostrar el hash completo (64 caracteres) para que el
# contraste sea evidente
display(df_diego)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
