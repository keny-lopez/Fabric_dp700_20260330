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

# CELL ********************

# Crear ficheros JSONcon data aleatoria:

import json
import os
import uuid
import random
import time
from datetime import datetime

# Configuración de ficheros:


carpeta_salida = "/lakehouse/default/Files/ficheros_json"
num_ficheros = 10
seg_espera = 3

# Verificación de existencia de carpetas:
os.makedirs(carpeta_salida, exist_ok=True)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for i in range(num_ficheros):
    # Variables
    now = datetime.utcnow()
    timestamp_str = now.strftime("%Y%m%dT%H%M%S")

    # Simulación de mensaje sensor
    registro = {
        "id": str(uuid.uuid4()),
        "temp": round(random.uniform(21.0, 35.0), 2),
        "timestamp": now.isoformat()
    }

    # Construir el fichero JSON
    filename = f"temp_{timestamp_str}.json"
    filepath = os.path.join(carpeta_salida, filename)

    # Materializar el fichero JSON
    with open(filepath, "w") as f:
        json.dump(registro, f)

    print(f"OK - [{i+1}/{num_ficheros}] Escribió: {filename}")
    time.sleep(seg_espera)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
