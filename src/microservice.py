import os
import io
import logging
from logging.handlers import TimedRotatingFileHandler
import requests
import pandas as pd
from flask import Flask, jsonify, make_response
from apscheduler.schedulers.background import BackgroundScheduler
from requests.exceptions import RequestException
import datetime

# ------------------------------------------------------------------------------
# CONFIGURACIÓN DE LOGS CON TimedRotatingFileHandler
# ------------------------------------------------------------------------------
log_file = "microservice.log"

log_handler = TimedRotatingFileHandler(
    log_file,
    when="midnight",
    interval=1,
    backupCount=7
)

log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
log_handler.setFormatter(log_formatter)

# Logger principal
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

# Adicionalmente, muestra logs en consola
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

# ------------------------------------------------------------------------------
# CONFIGURACIÓN DE ARTIFACTORY / ENTORNOS
# ------------------------------------------------------------------------------
# URL base (carpeta) donde se encuentran los CSV a descargar
ARTIFACTORY_DOCUMENTS_URL = os.getenv(
    "ARTIFACTORY_DOCUMENTS_URL",
    ""
).rstrip("/")

# Credenciales (ajusta si tu Artifactory las requiere)
ART_USER = os.getenv("ART_USER", "")
ART_PASSWORD = os.getenv("ART_PASSWORD", "")

# Tiempo de espera para las requests (segundos)
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", 10))

# Verificación de certificados SSL (poner True en producción con certificado válido)
VERIFY_SSL = False

# Intervalo en minutos para la tarea de refresco programada
SCHEDULE_INTERVAL = int(os.getenv("SCHEDULE_INTERVAL", 5))

# Lista de archivos CSV que se descargarán
FILES_TO_REFRESH = [
    "reporteFortify.csv",
    "reporteGithub.csv"
]

# ------------------------------------------------------------------------------
# CONFIGURACIÓN DE FLASK
# ------------------------------------------------------------------------------
app = Flask(__name__)

# ------------------------------------------------------------------------------
# CACHE EN MEMORIA
# ------------------------------------------------------------------------------
csv_cache = {}


# ------------------------------------------------------------------------------
# FUNCIÓN PARA DESCARGAR Y CONVERTIR UN CSV EN JSON (LISTA DE DICTS)
# ------------------------------------------------------------------------------
def descargar_y_convertir_csv(filename):
    url = f"{ARTIFACTORY_DOCUMENTS_URL}/{filename}"
    logger.info(f"🔗 Descargando CSV desde: {url}")

    try:
        response = requests.get(
            url,
            auth=(ART_USER, ART_PASSWORD),
            verify=VERIFY_SSL,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()

        df = pd.read_csv(io.StringIO(response.text), encoding="utf-8", low_memory=False)
        data_json = df.to_dict(orient="records")
        logger.info(f"✅ Descarga exitosa. Filas leídas: {len(df)}")
        return data_json

    except RequestException as re:
        logger.error(f"❌ Error al descargar el archivo CSV {filename}: {re}")
        return None
    except pd.errors.EmptyDataError:
        logger.error(f"⚠️ El CSV {filename} está vacío o corrompido.")
        return None
    except Exception as e:
        logger.exception(f"❌ Error inesperado al procesar el CSV {filename}: {e}")
        return None


# ------------------------------------------------------------------------------
# FUNCIÓN DE REFRESCO DE CACHE (PROGRAMADA)
# ------------------------------------------------------------------------------
def refrescar_cache():
    logger.info("⏳ Iniciando refresco automático de la caché...")
    csv_cache.clear()

    for filename in FILES_TO_REFRESH:
        logger.info(f"🔄 Refrescando archivo: {filename}")
        data_json = descargar_y_convertir_csv(filename)
        if data_json is not None:
            csv_cache[filename] = {
                "data": data_json,
                "last_updated": datetime.datetime.now()
            }
            logger.info(f"✅ Caché actualizada para: {filename}")
        else:
            logger.warning(f"⚠️ No se pudo refrescar datos para {filename}.")

    logger.info("✅ Refresco automático completado.")


# ------------------------------------------------------------------------------
# ENDPOINT PARA OBTENER DATA DE UN CSV DESDE LA CACHÉ (SIN DESCARGA AL VUELO)
# ------------------------------------------------------------------------------
@app.route("/api/v1/data/<path:filename>", methods=["GET"])
def get_csv_data(filename):
    if filename in csv_cache:
        logger.info(f"↩️ Devolviendo datos desde la caché para: {filename}")
        return jsonify(csv_cache[filename]["data"])
    else:
        logger.warning(f"⚠️ El archivo {filename} no está en la caché.")
        return make_response(
            jsonify({"error": f"No se encontraron datos para {filename} en la caché."}),
            404
        )

# ------------------------------------------------------------------------------
# ENDPOINT PARA VER ESTADO DE LA CACHÉ
# ------------------------------------------------------------------------------
@app.route("/api/v1/cache/status", methods=["GET"])
def cache_status():
    status = {
        filename: {
            "last_updated": (
                cache_data["last_updated"].isoformat()
                if "last_updated" in cache_data else "N/A"
            ),
            "entries": len(cache_data.get("data", []))
        }
        for filename, cache_data in csv_cache.items()
    }
    return jsonify(status)

# ------------------------------------------------------------------------------
# CONFIGURACIÓN DE APSCHEDULER
# ------------------------------------------------------------------------------
scheduler = BackgroundScheduler()

# La clave es usar next_run_time=datetime.datetime.now() para que el job corra
# inmediatamente al iniciar el microservicio y, después, cada SCHEDULE_INTERVAL.
scheduler.add_job(
    refrescar_cache,
    'interval',
    minutes=SCHEDULE_INTERVAL,
    next_run_time=datetime.datetime.now()
)

scheduler.start()

# ------------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("🚀 Iniciando microservicio Flask con actualización inmediata y automática...")
    # ¡No necesitamos llamar manualmente a refrescar_cache()!
    # El job se ejecutará de inmediato por 'next_run_time=datetime.datetime.now()'
    # y luego respetará el intervalo (SCHEDULE_INTERVAL).

    app.run(host="0.0.0.0", port=5000, debug=False)
