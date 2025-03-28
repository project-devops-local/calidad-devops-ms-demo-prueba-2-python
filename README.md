# CSV Microservice

Este microservicio lee un archivo CSV desde Artifactory, procesa los datos y expone métricas en Prometheus para ser visualizadas en Grafana.

## Características

- Descarga y procesamiento de archivos CSV.
- Exposición de métricas personalizadas en `/metrics`.
- Configuración para ser desplegado en Kubernetes.

## Requisitos

- Python 3.10+
- Docker
- Kubernetes (AKS recomendado)
- Prometheus y Grafana configurados

## Estructura del Proyecto

- `src/`: Contiene el código fuente del microservicio.
- `kubernetes/`: Archivos YAML para el despliegue en Kubernetes.
- `Dockerfile`: Archivo para construir la imagen Docker.
- `requirements.txt`: Lista de dependencias del proyecto.

## Uso

### Local

1. Instalar dependencias:
   ```bash
   pip install -r requirements.txt

#prueba de agregar
sadsdfsdfdsf