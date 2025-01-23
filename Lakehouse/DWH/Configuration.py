# Databricks notebook source
import psycopg2
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Obtener la contraseña de la base de datos desde Databricks
db_password = dbutils.secrets.get('KeyVaultSecretScope', 'psqlpwd')

# Función para establecer la conexión a PostgreSQL
def get_pg_connection():
    try:
        conn = psycopg2.connect(
            dbname="lakehouse",
            user="sqladminuser",
            password=db_password,
            host="psql-metrodoralakehouse-dev.postgres.database.azure.com",
            port="5432"
        )
        logger.info("Conexión a PostgreSQL establecida correctamente.")
        return conn
    except Exception as e:
        logger.error(f"Error al conectar a PostgreSQL: {e}")
        raise
