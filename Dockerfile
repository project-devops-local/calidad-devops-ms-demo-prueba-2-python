# 1) Imagen base
FROM python:3.10-slim

# 2) Directorio de trabajo
WORKDIR /app

# 3) Copiar archivos del microservicio
COPY ./src /app/src
COPY requirements.txt /app/

# 4) Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# 5) Exponer puerto (donde Gunicorn escuchará, p.ej. 5000)
EXPOSE 5000

WORKDIR /app/src
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "microservice:app"]
