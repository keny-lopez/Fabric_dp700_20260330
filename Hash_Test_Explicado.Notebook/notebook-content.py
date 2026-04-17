# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # 🔐 Hashing SHA-256 — Detección de Cambios en Dimensiones
# ### Microsoft Fabric · PySpark · Modern Data Warehouse
# <table style="border:none; background:transparent;"><tr>
# <td style="border:none; padding:0 12px 0 0;"><img src="https://flagcdn.com/48x36/pa.png" width="48" height="36"> Panamá</td>
# <td style="border:none; padding:0 12px;">·</td>
# <td style="border:none; padding:0 12px;"><img src="https://flagcdn.com/48x36/cr.png" width="48" height="36"> Costa Rica</td>
# <td style="border:none; padding:0 12px;">·</td>
# <td style="border:none; padding:0 12px;"><img src="https://flagcdn.com/48x36/hn.png" width="48" height="36"> Honduras</td>
# </tr></table>
# 
# ---
# 
# ## ¿Qué es este Notebook?
# 
# Este Notebook es una demostración práctica de una técnica de **Ingeniería de Datos**
# llamada **Hashing**, aplicada al problema de detectar cambios en dimensiones
# dentro de un **Modern Data Warehouse**.
# 
# ---
# 
# ## El problema que resuelve
# 
# En cualquier sistema de datos empresarial existe lo que se conoce como una
# **dimensión de cambio lento** o **SCD Tipo 2** (Slowly Changing Dimension Type 2).
# 
# Una dimensión es una tabla que describe entidades del negocio:
# clientes, productos, empleados, proveedores, etc.
# 
# El problema ocurre cuando esa tabla se actualiza periódicamente
# y necesitamos saber **qué registros cambiaron** entre una versión y otra.
# 
# ### La solución tradicional (y por qué falla)
# 
# La forma más obvia es comparar **columna por columna**:
# si el nombre cambió, si el email cambió, si el teléfono cambió...
# 
# Esto funciona con 3 o 4 columnas.
# Pero en una dimensión real con **50 o 100 columnas** y **millones de filas**,
# este método se vuelve:
# 
# - **Lento**: cada comparación adicional consume recursos de procesamiento.
# - **Frágil**: si se agrega una columna nueva a la tabla, hay que modificar el código.
# - **Difícil de mantener**: el código crece de forma proporcional al número de columnas.
# 
# ### La solución con Hashing
# 
# El **Hashing** resuelve este problema con una idea elegante:
# 
# > En lugar de comparar cada columna individualmente,
# > convertimos **todo el registro en una sola huella digital**.
# > Si la huella cambió → el registro cambió.
# > Si la huella es igual → no hubo ningún cambio.
# 
# Esta huella se genera con el algoritmo **SHA-256**,
# que produce una cadena de **64 caracteres hexadecimales**
# única para cada combinación de datos.
# 
# ---
# 
# ## Conceptos clave de este Notebook
# 
# | Concepto | Tipo | Descripción |
# |---|---|---|
# | **Hashing (SHA-256)** | Irreversible | Genera una huella única. No se puede revertir al dato original. |
# | **Tokenización** | Reversible | Sustituye el dato real por un token. Se puede recuperar con una llave. |
# | **SALT** | Protección | Valor aleatorio que se agrega antes de hashear para evitar ataques de diccionario. |
# 
# ---
# 
# ## Herramientas utilizadas
# 
# - **Microsoft Fabric**: Plataforma de datos en la nube de Microsoft.
# - **PySpark**: API de Python para Apache Spark, usada para procesar grandes volúmenes de datos.
# - **Python 3**: Lenguaje de programación base del Notebook.
# 
# ---
# 
# ## Estructura del Notebook
# 
# | Celda | Contenido |
# |---|---|
# | Celda 1 | Carga de datos — Tabla original de clientes (V1) |
# | Celda 2 | Simulación de cambios — Tabla actualizada (V2) |
# | Celda 3 | Identificación visual de cambios — ANTES vs DESPUES |
# | Celda 4 | El problema — Detección sin Hashing (método frágil) |
# | Celda 5 | La solución — Detección con Hashing SHA-256 |
# | Celda 6 | El momento "Eureka" — Un cambio mínimo, un hash completamente distinto |


# CELL ********************

# ============================================================
# CELDA 1 — Carga de datos: Tabla original de clientes (V1)
# ============================================================
#
# TÉCNICA: Creación de un DataFrame estructurado en PySpark
#
# CASO DE ESTUDIO:
#   Imaginemos que somos ingenieros de datos en una empresa
#   que opera en Panamá, Costa Rica y Honduras.
#   Nuestra base de datos tiene una tabla de clientes que
#   se actualiza periódicamente desde distintas fuentes.
#
#   Esta celda representa la fotografía inicial de esa tabla
#   — la versión que tenemos hoy, antes de recibir cualquier
#   actualización.
#
# PROBLEMA QUE PLANTEA:
#   Si mañana llega una versión nueva de esta tabla,
#   ¿cómo sabemos cuáles de estos 10 registros cambiaron?
#   ¿Y cómo lo sabemos de forma eficiente si la tabla
#   tuviera 20 millones de filas en lugar de 10?
# ============================================================

# ── Importaciones ──────────────────────────────────────────
#
# 'StructType' permite definir la estructura completa de una tabla,
# especificando el nombre, tipo y restricciones de cada columna.
#
# 'StructField' define cada columna individualmente dentro del schema.
# Recibe tres argumentos:
#   1. Nombre de la columna (string)
#   2. Tipo de dato (IntegerType, StringType, etc.)
#   3. Si acepta valores nulos (True = sí acepta, False = no acepta)
#
# 'StringType' es el tipo de dato para texto (nombre, email, etc.)
# 'IntegerType' es el tipo de dato para números enteros (cliente_id)
#
# NOTA: En Microsoft Fabric, el objeto 'spark' ya está disponible
# automáticamente. No es necesario importar SparkSession ni
# crear la sesión manualmente — Fabric lo hace por nosotros.

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# ── Datos de prueba ────────────────────────────────────────
#
# Definimos los datos como una lista de tuplas (filas).
# Cada tupla representa un registro completo de un cliente.
# El orden de los valores debe coincidir exactamente con
# el orden de las columnas definidas en el schema.
#
# Distribución geográfica (estratégica para visibilidad):
#   - IDs 1, 3, 8, 10 → Panamá        (+507)
#   - IDs 2, 4, 7, 9  → Costa Rica    (+506)
#   - IDs 5, 6        → Honduras      (+504)
#
# Esta distribución coloca a Panamá y Costa Rica al inicio
# y al final de la tabla, donde el ojo del lector se fija más.

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

# ── Definición del Schema ──────────────────────────────────
#
# El schema es el "contrato" de la tabla: define qué columnas
# existen, qué tipo de dato contiene cada una y si puede
# tener valores vacíos o nulos.
#
# StructType([...]) recibe una lista de StructField.
# Cada StructField define una columna:
#
#   cliente_id  → IntegerType, False (obligatorio, es la llave primaria)
#   nombre      → StringType,  True  (puede ser nulo en teoría)
#   email       → StringType,  True
#   telefono    → StringType,  True
#   ciudad      → StringType,  True
#
# Definir el schema explícitamente (en lugar de dejarlo inferir
# a Spark automáticamente) es una buena práctica porque:
#   1. Garantiza consistencia en los tipos de datos.
#   2. Mejora el rendimiento al evitar el paso de inferencia.
#   3. Hace el código más legible y autodocumentado.

schema = StructType([
    StructField("cliente_id", IntegerType(), False),  # Llave primaria — no puede ser nulo
    StructField("nombre",     StringType(),  True),
    StructField("email",      StringType(),  True),
    StructField("telefono",   StringType(),  True),
    StructField("ciudad",     StringType(),  True),
])

# ── Creación del DataFrame ─────────────────────────────────
#
# spark.createDataFrame(datos, schema) crea un DataFrame de Spark
# a partir de una lista de tuplas y un schema definido.
#
# Un DataFrame en Spark es similar a una tabla de base de datos
# o a una hoja de Excel, pero diseñada para procesar millones
# o miles de millones de filas de forma distribuida.
#
# 'df_v1' es el nombre de la variable que almacena este DataFrame.
# La convención 'v1' indica que es la Versión 1 (original).

df_v1 = spark.createDataFrame(datos_v1, schema)

# ── Registro como Vista Temporal ───────────────────────────
#
# createOrReplaceTempView("nombre") registra el DataFrame como
# una vista temporal en el contexto de la sesión de Spark.
#
# Esto permite consultar la tabla usando SQL en celdas posteriores:
#   spark.sql("SELECT * FROM dim_clientes_v1")
#
# La vista existe solo durante la sesión activa del Notebook.
# Al cerrar o reiniciar la sesión, la vista desaparece.
# Por eso se llama "temporal".

df_v1.createOrReplaceTempView("dim_clientes_v1")

# ── Visualización ──────────────────────────────────────────
#
# display() es una función nativa de Microsoft Fabric que
# renderiza el DataFrame como una tabla interactiva con:
#   - Columnas redimensionables
#   - Scroll horizontal y vertical
#   - Filtros por columna
#   - Ordenamiento con clic en el encabezado
#
# Es preferible a .show() que imprime texto plano no interactivo.
#
# RESULTADO ESPERADO:
#   Una tabla con 10 filas y 5 columnas:
#   cliente_id | nombre | email | telefono | ciudad
#   Todos los registros están en su estado original, sin cambios.

display(df_v1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELDA 2 — Simulación de cambios: Tabla actualizada (V2)
# ============================================================
#
# TÉCNICA: Simulación de actualizaciones en una dimensión SCD Tipo 2
#
# CASO DE ESTUDIO:
#   Han pasado 30 días desde la última carga de datos.
#   El sistema fuente envía una nueva versión de la tabla
#   de clientes con actualizaciones de sus registros.
#
#   En la realidad, estos cambios ocurren constantemente:
#   un cliente actualiza su email, cambia de ciudad,
#   o modifica su número de teléfono.
#
# CAMBIOS APLICADOS DELIBERADAMENTE:
#   Para demostrar la técnica, modificamos 3 registros
#   de forma intencional y controlada:
#
#   → Cliente ID 3  (Diego Martínez):  cambió su EMAIL
#                   Antes:  diego.martinez@email.com
#                   Después: diego.martinez.nuevo@email.com
#
#   → Cliente ID 7  (Luis Quesada):   cambió su TELÉFONO
#                   Antes:  +506-8807-7777
#                   Después: +506-8820-0000
#
#   → Cliente ID 9  (Pablo Solís):    cambió su CIUDAD
#                   Antes:  Cartago
#                   Después: San José
#
# PREGUNTA CLAVE:
#   Si tuviéramos 20 millones de filas en lugar de 10,
#   ¿cómo detectaríamos estos 3 cambios de forma eficiente?
#   Esa pregunta es exactamente lo que resuelve este Notebook.
# ============================================================

# Los datos son idénticos a V1, excepto en los 3 registros
# marcados con el comentario "# cambió" al final de la línea.
# Esta anotación facilita la revisión visual del código.

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

# ── Creación del DataFrame V2 ──────────────────────────────
#
# Reutilizamos el mismo 'schema' definido en la Celda 1.
# Esto garantiza que ambas tablas tienen exactamente la misma
# estructura, lo cual es un requisito para compararlas.
#
# Si los schemas fueran diferentes, la comparación fallaría
# o produciría resultados incorrectos.

df_v2 = spark.createDataFrame(datos_v2, schema)

# ── Registro como Vista Temporal ───────────────────────────
#
# Registramos V2 como vista temporal con un nombre distinto
# a V1 para poder referenciar ambas tablas simultáneamente
# en consultas SQL o transformaciones de PySpark.

df_v2.createOrReplaceTempView("dim_clientes_v2")

# ── Visualización ──────────────────────────────────────────
#
# RESULTADO ESPERADO:
#   Una tabla con 10 filas idéntica a V1 en apariencia,
#   pero con 3 diferencias sutiles en los registros 3, 7 y 9.
#   A simple vista puede ser difícil notar los cambios —
#   eso es exactamente el problema que vamos a resolver.

display(df_v2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELDA 3 — Identificación de cambios: ANTES vs DESPUES
# ============================================================
#
# TÉCNICA: Join entre DataFrames + Filtro condicional
#
# CASO DE ESTUDIO:
#   Antes de aplicar Hashing, queremos ver visualmente
#   cuáles registros cambiaron y exactamente qué campo
#   fue modificado en cada caso.
#
#   Esta celda responde la pregunta:
#   "¿Qué cambió, en quién y en qué campo?"
#
# CÓMO FUNCIONA:
#   1. Une V1 y V2 por cliente_id (el identificador único).
#   2. Filtra solo los registros donde al menos un campo
#      es diferente entre ambas versiones.
#   3. Muestra el valor ANTES (de V1) y DESPUES (de V2)
#      para cada campo, lado a lado.
# ============================================================

# ── Importación ────────────────────────────────────────────
#
# 'col' es una función de PySpark que referencia una columna
# de un DataFrame por su nombre como string.
# Ejemplo: col("email") referencia la columna "email".
#
# Es equivalente a escribir df["email"] pero más flexible
# cuando trabajamos con múltiples DataFrames simultáneamente,
# ya que permite especificar el alias de la tabla:
# col("v1.email") vs col("v2.email")

from pyspark.sql.functions import col

# ── Join entre V1 y V2 ─────────────────────────────────────
#
# .alias("v1") y .alias("v2") asignan nombres temporales
# a cada DataFrame para distinguir sus columnas durante el join.
# Sin estos alias, PySpark no sabría si "email" viene de V1 o V2.
#
# .join(df_v2.alias("v2"), on="cliente_id") realiza un
# INNER JOIN entre ambas tablas usando cliente_id como llave.
# Solo incluye registros que existen en AMBAS tablas.
#
# .filter(...) aplica una condición para quedarse solo con
# los registros donde al menos un campo difiere entre V1 y V2.
# El operador '|' significa OR lógico.
#
# .select(...) elige qué columnas mostrar en el resultado.
# .alias("email_ANTES") renombra la columna para claridad visual.

df_diff = df_v1.alias("v1").join(df_v2.alias("v2"), on="cliente_id") \
    .filter(
        (col("v1.email")    != col("v2.email"))    |  # email diferente
        (col("v1.telefono") != col("v2.telefono")) |  # teléfono diferente
        (col("v1.ciudad")   != col("v2.ciudad"))      # ciudad diferente
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

# ── Visualización ──────────────────────────────────────────
#
# RESULTADO ESPERADO:
#   Solo 3 filas — los clientes 3, 7 y 9.
#   Cada fila muestra el valor ANTES y DESPUES de cada campo.
#   Los campos que no cambiaron muestran el mismo valor en ambas columnas.
#   Los campos que sí cambiaron muestran valores distintos.
#
# LIMITACIÓN DE ESTE MÉTODO:
#   Para generar este resultado tuvimos que saber de antemano
#   qué columnas podían cambiar (email, teléfono, ciudad).
#   En una tabla real con 100 columnas, tendríamos que escribir
#   100 condiciones en el .filter(). Eso es exactamente
#   el problema que resuelve el Hashing en la Celda 5.

display(df_diff)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELDA 4 — El problema: Detección de cambios SIN Hashing
# ============================================================
#
# TÉCNICA: Comparación columna por columna con withColumn + when
#
# CASO DE ESTUDIO:
#   Esta celda demuestra el método tradicional de detección
#   de cambios: comparar cada columna individualmente y
#   etiquetar si cambió o no.
#
# POR QUÉ ES UN PROBLEMA:
#   Este código funciona correctamente para 3 columnas.
#   Pero revela su fragilidad cuando escalamos:
#
#   3 columnas   → 3 condiciones  (manejable)
#   10 columnas  → 10 condiciones (tedioso)
#   50 columnas  → 50 condiciones (propenso a errores)
#   100 columnas → 100 condiciones (inmantenible)
#
#   Además, si el equipo de negocio agrega una columna nueva
#   a la tabla (por ejemplo, "fecha_nacimiento"), un ingeniero
#   debe recordar agregar esa comparación al código manualmente.
#   Si lo olvida, esa columna nunca se detectará como cambiada.
#
# OBJETIVO DE ESTA CELDA:
#   Crear el contraste visual y técnico que justifica
#   la solución con Hashing en la siguiente celda.
# ============================================================

# ── Importaciones ──────────────────────────────────────────
#
# 'when' es una función condicional similar al IF de Excel o SQL.
# Sintaxis: when(condición, valor_si_verdadero).otherwise(valor_si_falso)
# Ejemplo: when(col("v1.email") != col("v2.email"), "CAMBIÓ").otherwise("igual")
#
# 'lit' crea una columna de valor literal (constante).
# Es necesario cuando queremos asignar un texto fijo como resultado.
# Sin 'lit', PySpark interpretaría "CAMBIÓ" como el nombre de una columna.

from pyspark.sql.functions import when, lit

# ── Comparación columna por columna ────────────────────────
#
# .withColumn("nombre_columna", expresión) agrega una nueva columna
# al DataFrame con el nombre y valor especificados.
#
# Aquí creamos una columna de "estado" por cada campo comparado:
#   - cambio_email:    "CAMBIÓ" si los emails difieren, "igual" si no.
#   - cambio_telefono: "CAMBIÓ" si los teléfonos difieren, "igual" si no.
#   - cambio_ciudad:   "CAMBIÓ" si las ciudades difieren, "igual" si no.
#
# NOTA: Observa cómo se repite la misma estructura tres veces.
# Para 100 columnas, esta sección tendría 100 bloques idénticos.
# Eso es exactamente lo que hace este método frágil e inmantenible.

df_comparacion = df_v1.alias("v1").join(df_v2.alias("v2"), on="cliente_id") \
    .withColumn("cambio_email",
        when(col("v1.email")    != col("v2.email"),    lit("CAMBIÓ")).otherwise(lit("igual"))) \
    .withColumn("cambio_telefono",
        when(col("v1.telefono") != col("v2.telefono"), lit("CAMBIÓ")).otherwise(lit("igual"))) \
    .withColumn("cambio_ciudad",
        when(col("v1.ciudad")   != col("v2.ciudad"),   lit("CAMBIÓ")).otherwise(lit("igual"))) \
    .select(
        col("cliente_id"),
        col("v1.nombre"),
        col("cambio_email"),
        col("cambio_telefono"),
        col("cambio_ciudad"),
    )

# ── Visualización ──────────────────────────────────────────
#
# RESULTADO ESPERADO:
#   Una tabla con 10 filas y 5 columnas.
#   Los clientes 3, 7 y 9 muestran "CAMBIÓ" en su campo modificado.
#   Los demás muestran "igual" en todas las columnas de estado.
#
# REFLEXIÓN:
#   El resultado es correcto. El método funciona.
#   Pero el código que lo produce es largo, repetitivo y frágil.
#   En la siguiente celda veremos cómo reducirlo a una sola
#   comparación sin importar cuántas columnas tenga la tabla.

display(df_comparacion)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELDA 5 — La solución: Detección de cambios CON Hashing
# ============================================================
#
# TÉCNICA: Hashing SHA-256 con sha2() y concat_ws() de PySpark
#
# CASO DE ESTUDIO:
#   En lugar de comparar cada columna individualmente,
#   convertimos todo el registro en una sola "huella digital"
#   usando el algoritmo SHA-256.
#
#   Si dos registros son idénticos → sus huellas son idénticas.
#   Si un registro cambió en cualquier campo → su huella cambia.
#
# QUÉ ES SHA-256:
#   SHA-256 (Secure Hash Algorithm 256 bits) es un algoritmo
#   criptográfico que toma cualquier texto como entrada y
#   produce una cadena de exactamente 64 caracteres hexadecimales.
#
#   Propiedades clave:
#   → DETERMINISTA: el mismo texto siempre produce el mismo hash.
#   → IRREVERSIBLE: no se puede recuperar el texto original desde el hash.
#   → SENSIBLE: un cambio mínimo en el texto produce un hash completamente distinto.
#   → ÚNICO: la probabilidad de que dos textos distintos produzcan el mismo hash
#     es matemáticamente insignificante (colisión).
#
# VENTAJA SOBRE EL MÉTODO ANTERIOR:
#   Sin importar si la tabla tiene 3 columnas o 300,
#   este código NO cambia. Solo se actualiza la lista COLUMNAS.
# ============================================================

# ── Importaciones ──────────────────────────────────────────
#
# 'sha2(columna, bits)' aplica el algoritmo SHA-2 a una columna.
#   El segundo argumento especifica la longitud en bits del hash.
#   256 → SHA-256 (produce 64 caracteres hexadecimales).
#   512 → SHA-512 (produce 128 caracteres, más seguro pero más largo).
#
# 'concat_ws(separador, *columnas)' concatena múltiples columnas
#   en un solo texto, separadas por el separador especificado.
#   Ejemplo: concat_ws("|", "Ana", "ana@email.com", "+507-1234")
#   Produce: "Ana|ana@email.com|+507-1234"
#
#   El separador "|" es importante porque evita que dos registros
#   distintos produzcan el mismo texto concatenado.
#   Sin separador: "Ana" + "Garcia" = "AnaGarcia"
#                  "A"   + "naGarcia" = "AnaGarcia" (¡colisión!)
#   Con separador: "Ana|Garcia" ≠ "A|naGarcia"
#
# 'substring(columna, inicio, longitud)' extrae una parte del texto.
#   Lo usamos para mostrar solo los primeros 20 caracteres del hash
#   y que la tabla sea legible en pantalla.
#   El hash completo sigue siendo de 64 caracteres internamente.
#
# 'when' y 'lit' ya fueron explicados en la Celda 4.

from pyspark.sql.functions import sha2, concat_ws, substring, when, lit, col

# ── Configuración ──────────────────────────────────────────
#
# COLUMNAS define qué campos forman parte de la huella digital.
# Esta lista es el único lugar que necesita actualizarse si
# la tabla agrega o elimina columnas en el futuro.
#
# SEPARADOR es el carácter que une los valores de cada columna
# antes de aplicar el hash. Usar "|" es una convención común
# porque raramente aparece dentro de los datos reales.

COLUMNAS  = ["nombre", "email", "telefono", "ciudad"]
SEPARADOR = "|"

# ── Generación de huellas digitales para V1 ────────────────
#
# .withColumn("hash_registro", sha2(concat_ws(...), 256)) agrega
# una nueva columna llamada "hash_registro" a df_v1.
#
# El proceso interno para cada fila es:
#   1. concat_ws("|", col("nombre"), col("email"), ...) une todos
#      los campos en un solo texto:
#      "Andrés Fábrega|andres.fabrega@email.com|+507-6601-1111|Ciudad de Panamá"
#
#   2. sha2(..., 256) aplica SHA-256 a ese texto y produce:
#      "f13084a192884fcd4248703afbd83d310c5713ea62a98d7ce89346bfe00c1ed2"
#
# *[col(c) for c in COLUMNAS] es una expresión Python que
# expande la lista COLUMNAS en argumentos individuales para concat_ws.
# Es equivalente a escribir: col("nombre"), col("email"), col("telefono"), col("ciudad")
# pero funciona automáticamente sin importar cuántas columnas haya.

df_v1_hash = df_v1.withColumn(
    "hash_registro",
    sha2(concat_ws(SEPARADOR, *[col(c) for c in COLUMNAS]), 256)
)

# ── Generación de huellas digitales para V2 ────────────────
#
# Exactamente el mismo proceso aplicado a df_v2.
# Los registros que no cambiaron producirán el mismo hash que en V1.
# Los registros que sí cambiaron producirán un hash completamente distinto.

df_v2_hash = df_v2.withColumn(
    "hash_registro",
    sha2(concat_ws(SEPARADOR, *[col(c) for c in COLUMNAS]), 256)
)

# ── Comparación de huellas ─────────────────────────────────
#
# Aquí está la elegancia del método:
# En lugar de comparar columna por columna, comparamos UNA SOLA columna.
#
# .join une V1 y V2 por cliente_id (igual que en celdas anteriores).
#
# .withColumn("estado", when(...)) crea la columna de resultado:
#   Si hash_v1 ≠ hash_v2 → "🔴 CAMBIÓ"   (el registro fue modificado)
#   Si hash_v1 = hash_v2 → "✅ SIN CAMBIOS" (el registro es idéntico)
#
# .withColumn("hash_v1", substring(..., 1, 20)) recorta el hash
# a 20 caracteres solo para visualización.
# El "1" indica que empieza desde el primer carácter.
# El "20" indica cuántos caracteres mostrar.

df_resultado = df_v1_hash.alias("v1").join(
    df_v2_hash.alias("v2"), on="cliente_id"
) \
.withColumn(
    "estado",
    when(col("v1.hash_registro") != col("v2.hash_registro"), lit("🔴 CAMBIÓ"))
    .otherwise(lit("✅ SIN CAMBIOS"))
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

# ── Visualización ──────────────────────────────────────────
#
# .orderBy("cliente_id") ordena el resultado por ID ascendente
# para facilitar la lectura y comparación visual.
#
# RESULTADO ESPERADO:
#   10 filas ordenadas por cliente_id.
#   Clientes 1, 2, 4, 5, 6, 8, 10 → ✅ SIN CAMBIOS (hash_v1 = hash_v2)
#   Clientes 3, 7, 9               → 🔴 CAMBIÓ     (hash_v1 ≠ hash_v2)
#
# CONCLUSIÓN:
#   Con una sola columna comparada detectamos exactamente
#   los mismos 3 cambios que en la Celda 4, pero con código
#   que funciona igual para 3 columnas que para 300.

display(df_resultado.orderBy("cliente_id"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELDA 6 — El momento "ajá": un cambio mínimo, hash distinto
# ============================================================
#
# TÉCNICA: Aislamiento de un registro para análisis comparativo
#
# CASO DE ESTUDIO:
#   Una de las propiedades más importantes de SHA-256 es su
#   EFECTO AVALANCHA: un cambio mínimo en el texto de entrada
#   produce un hash completamente diferente en la salida.
#
#   Esto garantiza que ningún cambio pase desapercibido,
#   sin importar qué tan pequeño o sutil sea.
#
# QUÉ DEMOSTRAREMOS:
#   Diego Martínez cambió su email agregando ".nuevo" antes del "@".
#   Visualmente, los emails se ven casi idénticos:
#     Antes:   diego.martinez@email.com       (27 caracteres)
#     Después: diego.martinez.nuevo@email.com (33 caracteres)
#
#   Sin embargo, sus hashes SHA-256 son completamente distintos,
#   sin ninguna relación visible entre sí.
#
# POR QUÉ ESTO IMPORTA:
#   En ciberseguridad, esta propiedad garantiza que un atacante
#   no puede "adivinar" el hash original modificando ligeramente
#   un dato conocido. Cada variación produce un resultado
#   impredecible y único.
# ============================================================

# ── Aislamiento del registro de Diego Martínez ─────────────
#
# Construimos un DataFrame que muestra solo el cliente_id 3
# (Diego Martínez) con sus datos y hashes de ambas versiones.
#
# .filter(col("cliente_id") == 3) filtra solo la fila con ID 3.
# Es equivalente a WHERE cliente_id = 3 en SQL.
#
# .select(...) selecciona las columnas relevantes para la comparación:
#   - nombre: para identificar al cliente
#   - email de V1 y V2: para ver el cambio original
#   - hash de V1 y V2: para ver el efecto avalancha

df_diego = df_v1_hash.alias("v1").join(
    df_v2_hash.alias("v2"), on="cliente_id"
) \
.filter(col("cliente_id") == 3) \
.select(
    col("v1.nombre"),
    col("v1.email").alias("email_V1"),
    col("v2.email").alias("email_V2"),
    col("v1.hash_registro").alias("hash_V1"),  # Hash completo de 64 caracteres
    col("v2.hash_registro").alias("hash_V2"),  # Hash completo de 64 caracteres
)

# ── Visualización ──────────────────────────────────────────
#
# Mostramos el hash COMPLETO de 64 caracteres (no recortado)
# para que el contraste entre ambos hashes sea evidente.
#
# RESULTADO ESPERADO:
#   Una sola fila con los datos de Diego Martínez.
#
#   email_V1: diego.martinez@email.com
#   email_V2: diego.martinez.nuevo@email.com
#
#   hash_V1: 7a2a7d9a4bab9a799e698d07bfe7701d... (64 caracteres)
#   hash_V2: 38feaee3316abe7195e26add059f9957... (64 caracteres)
#
#   Observa que aunque el email cambió solo unos caracteres,
#   los hashes no tienen ninguna similitud entre sí.
#   Eso es el efecto avalancha de SHA-256 en acción.
#
# CONCLUSIÓN FINAL DEL NOTEBOOK:
#   El Hashing SHA-256 es una técnica confiable, eficiente
#   y escalable para detectar cambios en dimensiones de datos.
#   Una sola columna reemplaza cientos de comparaciones,
#   y ningún cambio — por mínimo que sea — puede pasar
#   desapercibido.

display(df_diego)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# 
# ## Conclusiones
# 
# ### Lo que aprendimos en este Notebook
# 
# **1. El método tradicional tiene un límite.**
# Comparar columna por columna funciona a pequeña escala,
# pero se vuelve inmantenible en tablas con decenas de columnas
# y millones de filas. Es código frágil esperando romperse.
# 
# **2. El Hashing resuelve el problema con elegancia.**
# Una sola columna `hash_registro` representa el estado completo
# de un registro. Si cambia cualquier campo, el hash cambia.
# El código de comparación nunca necesita actualizarse.
# 
# **3. No todo Hashing es igual.**
# 
# | Técnica | ¿Reversible? | Uso principal |
# |---|---|---|
# | SHA-256 | ❌ No | Detectar cambios, verificar integridad |
# | Tokenización | ✅ Sí | Proteger datos con posibilidad de recuperación |
# | SALT + SHA-256 | ❌ No | Proteger datos sensibles contra ataques de diccionario |
# 
# **4. El efecto avalancha es tu aliado.**
# Un cambio de un solo carácter en cualquier campo produce
# un hash completamente distinto. Ningún cambio pasa desapercibido.
# 
# ---
# 
# ### Próximos pasos sugeridos
# 
# - Implementar esta técnica en un pipeline real de Microsoft Fabric
# - Explorar el uso de SALT para proteger columnas sensibles como emails
# - Comparar el rendimiento de Hashing vs comparación columna por columna
#   en tablas con millones de registros reales
# 
# ---
# 
# *Notebook creado en Microsoft Fabric · PySpark · SHA-256*
# *🇵🇦 Panamá · 🇨🇷 Costa Rica · 🇭🇳 Honduras*

