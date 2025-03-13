# Databricks notebook source
# DBTITLE 1,Establecer la conexión a PostgreSQL.
# MAGIC %run "../DWH/Configuration"

# COMMAND ----------

# DBTITLE 1,Insert dim_escenario_budget lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `dim_vertical`
def upsert_dim_vertical(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión a PostgreSQL
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO dim_vertical (
            id_Dim_Vertical, nombre_Vertical, nombre_Vertical_Corto, ETLcreatedDate, ETLupdatedDate
        )
        VALUES %s
        ON CONFLICT (id_Dim_Vertical) DO UPDATE SET
            nombre_Vertical = EXCLUDED.nombre_Vertical,
            nombre_Vertical_Corto = EXCLUDED.nombre_Vertical_Corto,
            ETLupdatedDate = EXCLUDED.ETLupdatedDate;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_Dim_Vertical"], row["nombre_Vertical"], row["nombre_Vertical_Corto"],
            row["ETLcreatedDate"], row["ETLupdatedDate"]
        ) for row in partition]

        if values:
            # Ejecutar la inserción o actualización en lotes
            execute_values(cursor, query, values)
            conn.commit()
            logger.info(f"Procesados {len(values)} registros en PostgreSQL (insertados o actualizados).")
        else:
            logger.info("No se encontraron datos válidos en esta partición.")

        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error al procesar registros: {e}")
        raise

# Leer datos desde la tabla `gold_lakehouse.dim_vertical` en Databricks
source_table = (spark.table("gold_lakehouse.dim_vertical")
                .select("id_Dim_Vertical", "nombre_Vertical", "nombre_Vertical_Corto",
                        "ETLcreatedDate", "ETLupdatedDate"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_dim_vertical)
    logger.info("Proceso completado con éxito (Upsert en dim_vertical de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")

