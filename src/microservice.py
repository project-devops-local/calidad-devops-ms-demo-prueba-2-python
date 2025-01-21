import os
import io
import logging
from logging.handlers import TimedRotatingFileHandler
import requests
from flask import Flask, Response
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
ARTIFACTORY_DOCUMENTS_URL = os.getenv("ARTIFACTORY_DOCUMENTS_URL", "").rstrip("/")
ART_USER = os.getenv("ART_USER", "")
ART_PASSWORD = os.getenv("ART_PASSWORD", "")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", 10))
VERIFY_SSL = False
SCHEDULE_INTERVAL = int(os.getenv("SCHEDULE_INTERVAL", 5))

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
# FUNCIÓN PARA DESCARGAR EL CSV
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
        total_lineas = csv_content.count('\n')
        logger.info(f"✅ Descarga exitosa de {filename}. Líneas leídas: {total_lineas}")
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
# ENDPOINT PRINCIPAL PARA OBTENER EL CSV
# ------------------------------------------------------------------------------
@app.route("/api/v1/data/<path:filename>", methods=["GET"])
def get_csv_data(filename):
    """
    Devuelve el contenido crudo del CSV (text/csv) directamente desde la caché.
    Si no existe en la caché, se responde con texto plano de error (no JSON).
    """
    cache_item = csv_cache.get(filename)

    if not cache_item:
        logger.warning(f"⚠️ El archivo {filename} no está en la caché.")
        return Response(
            f"Archivo '{filename}' no encontrado en la caché.\n",
            status=404,
            mimetype="text/plain"
        )

    logger.info(f"↩️ Devolviendo datos desde la caché para: {filename}")
    return Response(
        cache_item["content"],
        mimetype="text/csv"
    )

# ------------------------------------------------------------------------------
# ENDPOINT PARA VER ESTADO DE LA CACHÉ (SI AÚN QUIERES VERLO EN JSON)
# ------------------------------------------------------------------------------
@app.route("/api/v1/cache/status", methods=["GET"])
def cache_status():
    """
    Muestra (en JSON) cuándo fue la última actualización de cada archivo
    y cuántos caracteres tiene. Esto puede ser útil para diagnóstico.
    """
    status_info = {}
    for fname, data in csv_cache.items():
        lu = data["last_updated"]
        status_info[fname] = {
            "last_updated": lu.isoformat() if lu else "N/A",
            "characters_in_csv": len(data["content"]),
        }
    # Si NO quieres JSON en absoluto, puedes devolver CSV o texto plano.
    # Aquí mantengo JSON solo como diagnóstico.
    from flask import jsonify
    return jsonify(status_info)

# ------------------------------------------------------------------------------
# CONFIGURACIÓN DE APSCHEDULER
# ------------------------------------------------------------------------------
scheduler = BackgroundScheduler()
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
    logger.info("🚀 Iniciando microservicio Flask para exponer CSV en crudo...")
    app.run(host="0.0.0.0", port=5000, debug=False)
