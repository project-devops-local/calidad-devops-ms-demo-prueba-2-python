import os
import io
import logging
from logging.handlers import TimedRotatingFileHandler
import requests
from flask import Flask, make_response, jsonify, Response
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
ARTIFACTORY_DOCUMENTS_URL = os.getenv("ARTIFACTORY_DOCUMENTS_URL", "").rstrip("/")

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
# FUNCIÓN PARA DESCARGAR EL CSV SIN CONVERTIRLO A JSON
# ------------------------------------------------------------------------------
def descargar_csv(filename):
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

        csv_content = response.text
        # Puedes loguear cuántas líneas tiene el CSV, si quieres
        total_lineas = csv_content.count('\n')
        logger.info(f"✅ Descarga exitosa. Líneas leídas: {total_lineas}")
        return csv_content

    except RequestException as re:
        logger.error(f"❌ Error al descargar el archivo CSV {filename}: {re}")
        return None
    except Exception as e:
        logger.exception(f"❌ Error inesperado al procesar el CSV {filename}: {e}")
        return None

# ------------------------------------------------------------------------------
# FUNCIÓN DE REFRESCO DE CACHE (PROGRAMADA)
# ------------------------------------------------------------------------------
def refrescar_cache():
    logger.info("⏳ Iniciando refresco automático de la caché...")
    # Limpiamos la caché antes de refrescar
    csv_cache.clear()

    for filename in FILES_TO_REFRESH:
        logger.info(f"🔄 Refrescando archivo: {filename}")
        csv_content = descargar_csv(filename)
        if csv_content is not None:
            csv_cache[filename] = {
                "content": csv_content,
                "last_updated": datetime.datetime.now()
            }
            logger.info(f"✅ Caché actualizada para: {filename}")
        else:
            logger.warning(f"⚠️ No se pudo refrescar datos para {filename}.")

    logger.info("✅ Refresco automático completado.")

# ------------------------------------------------------------------------------
# ENDPOINT PARA OBTENER EL CSV DESDE LA CACHÉ
# ------------------------------------------------------------------------------
@app.route("/api/v1/data/<path:filename>", methods=["GET"])
def get_csv_data(filename):
    """Devuelve el contenido crudo del CSV (text/csv) directamente desde la caché."""
    if filename in csv_cache:
        logger.info(f"↩️ Devolviendo datos desde la caché para: {filename}")
        # Retornamos directamente el contenido en formato CSV.
        # Grafana o cualquier otra herramienta puede consumirlo.
        return Response(
            csv_cache[filename]["content"],
            mimetype="text/csv"
        )
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
    status = {}
    for filename, cache_data in csv_cache.items():
        last_updated = cache_data.get("last_updated")
        status[filename] = {
            "last_updated": last_updated.isoformat() if last_updated else "N/A",
            "characters_in_csv": len(cache_data["content"])
        }
    return jsonify(status)

# ------------------------------------------------------------------------------
# CONFIGURACIÓN DE APSCHEDULER
# ------------------------------------------------------------------------------
scheduler = BackgroundScheduler()

# Programamos el refresco inmediato al iniciar, y luego cada SCHEDULE_INTERVAL minutos
scheduler.add_job(
    refrescar_cache,
    'interval',
    minutes=SCHEDULE_INTERVAL,
    next_run_time=datetime.datetime.now()  # Se ejecuta de inmediato al iniciar
)

scheduler.start()

# ------------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("🚀 Iniciando microservicio Flask para exponer CSV en crudo...")
    # Con next_run_time=datetime.datetime.now(), se dispara el refresco inmediatamente
    app.run(host="0.0.0.0", port=5000, debug=False)
