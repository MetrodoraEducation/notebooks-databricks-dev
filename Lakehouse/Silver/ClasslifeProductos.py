# Databricks notebook source
# DBTITLE 1,ulac
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

endpoint_process_name = "enroll_groups"
table_name = "JsaClassLifeTitulaciones"

classlifetitulaciones_df = spark.read.json(f"{bronze_folder_path}/lakehouse/classlife/{endpoint_process_name}/{current_date}/{table_name}.json")
classlifetitulaciones_df

# COMMAND ----------

# MAGIC %md
# MAGIC **Pasos principales:**
# MAGIC - Limpiar el esquema completo del DataFrame antes de trabajar con las columnas anidadas.
# MAGIC - Desanidar las estructuras una por una asegurando que no existan conflictos.
# MAGIC - Revisar si existen estructuras complejas adicionales que deban manejarse de forma especial.

# COMMAND ----------

# DBTITLE 1,Inspeccionar y deduplicar columnas justo después de la lectura del JSON
# Inspeccionar y deduplicar columnas justo después de la lectura del JSON
def resolve_ambiguity(df):
    """
    Inspecciona y elimina columnas duplicadas para evitar ambigüedad.
    """
    column_names = df.columns
    duplicate_columns = {col: column_names.count(col) for col in column_names if column_names.count(col) > 1}

    if duplicate_columns:
        print(f"Columnas duplicadas detectadas: {duplicate_columns}")
        # Renombrar automáticamente las columnas duplicadas agregando un sufijo
        for col_name, count in duplicate_columns.items():
            for i in range(1, count):
                old_name = col_name
                new_name = f"{col_name}_renamed_{i}"
                df = df.withColumnRenamed(old_name, new_name)
    
    return df

# Resolver ambigüedad después de cargar los datos
classlifetitulaciones_df = resolve_ambiguity(classlifetitulaciones_df)

# COMMAND ----------

# DBTITLE 1,# Resolver ambigüedad y limpiar nombres de columnas
# Resolver ambigüedad y limpiar nombres de columnas
classlifetitulaciones_df = resolve_ambiguity_and_clean(classlifetitulaciones_df)

# COMMAND ----------

# DBTITLE 1,Select data to dataframe
classlifetitulaciones_df = classlifetitulaciones_df.select("data")

# COMMAND ----------

# DBTITLE 1,# Limpieza de nombres de columnas en el esquema
# Limpieza de nombres de columnas en el esquema
classlifetitulaciones_df = apply_cleaned_schema(classlifetitulaciones_df)

# COMMAND ----------

# DBTITLE 1,Desanidar completamente el DataFrame
# Desanidar completamente el DataFrame
classlifetitulaciones_df = flatten_dataframe(classlifetitulaciones_df)

# COMMAND ----------

# DBTITLE 1,Resolver ambigüedad después de desanidar
# Resolver ambigüedad después de desanidar
classlifetitulaciones_df = resolve_ambiguity(classlifetitulaciones_df)

# COMMAND ----------

# DBTITLE 1,Limpiar nombres de columnas y convertir a minúsculas
# Limpiar nombres de columnas y convertir a minúsculas
classlifetitulaciones_df = classlifetitulaciones_df.select(
    *[col for col in classlifetitulaciones_df.columns]  # Seleccionar columnas deduplicadas
)
for col in classlifetitulaciones_df.columns:
    classlifetitulaciones_df = classlifetitulaciones_df.withColumnRenamed(col, col.lower())

# COMMAND ----------

# DBTITLE 1,Limpieza de columnas
from pyspark.sql.types import *
from pyspark.sql.functions import col

# Función para deduplicar y limpiar nombres de columnas
def deduplicate_and_clean_columns(df):
    """
    Deduplica columnas, limpia caracteres especiales y resuelve problemas de ambigüedad en los nombres de columnas.
    """
    # Inspeccionar columnas duplicadas
    column_names = df.columns
    duplicate_columns = {col: column_names.count(col) for col in column_names if column_names.count(col) > 1}

    if duplicate_columns:
        print(f"Columnas duplicadas detectadas: {duplicate_columns}")

    # Deduplicar columnas conflictivas
    deduplicated_cols = []
    for col_name in column_names:
        if column_names.count(col_name) > 1:
            # Renombrar columnas duplicadas agregando un sufijo
            count = deduplicated_cols.count(col_name)
            new_col_name = f"{col_name}_renamed_{count + 1}"
            deduplicated_cols.append(new_col_name)
        else:
            deduplicated_cols.append(col_name)

    # Crear un nuevo DataFrame con columnas deduplicadas
    df = df.toDF(*deduplicated_cols)

    # Limpieza de nombres de columnas
    cleaned_columns = []
    for col_name in df.columns:
        new_col_name = (col_name
                        .replace("data_", "")
                        .replace("data_items_", "")
                        .replace("items_", "")
                        .replace("items_metas_", "")
                        .replace("metas_", "")
                        .replace("ñ", "n")
                        .replace(" ", "_")
                        .replace("ú", "u")
                        .replace("í", "i")
                        .replace("á", "a")
                        .replace("é", "e")
                        .replace("ó", "o")
                        .lower())  # Convertir a minúsculas para consistencia

        # Evitar duplicados después de la limpieza
        if new_col_name in cleaned_columns:
            count = cleaned_columns.count(new_col_name)
            new_col_name = f"{new_col_name}_renamed_{count + 1}"

        cleaned_columns.append(new_col_name)

    # Renombrar el DataFrame con los nuevos nombres
    df = df.toDF(*cleaned_columns)
    
    return df

# Aplicar deduplicación y limpieza de columnas
classlifetitulaciones_df = deduplicate_and_clean_columns(classlifetitulaciones_df)

# Mostrar el DataFrame resultante
display(classlifetitulaciones_df)


# COMMAND ----------

# DBTITLE 1,Reemplaza valores nulos en columnas basadas en sus tipos de datos
from pyspark.sql.functions import coalesce, lit, col

# Reemplaza valores nulos en columnas basadas en sus tipos de datos para classlifetitulaciones_df
for column_name, column_type in classlifetitulaciones_df.dtypes:
    if column_type == 'string':
        classlifetitulaciones_df = classlifetitulaciones_df.withColumn(column_name, coalesce(col(column_name), lit('')))
    elif column_type in ['double', 'float']:
        classlifetitulaciones_df = classlifetitulaciones_df.withColumn(column_name, coalesce(col(column_name), lit(0.0)))
    elif column_type in ['int', 'bigint']:
        classlifetitulaciones_df = classlifetitulaciones_df.withColumn(column_name, coalesce(col(column_name), lit(0)))
    elif column_type == 'boolean':
        classlifetitulaciones_df = classlifetitulaciones_df.withColumn(column_name, coalesce(col(column_name), lit(False)))
    elif column_type in ['timestamp', 'date']:
        # Para fechas y timestamps dejamos un valor explícito `1970-01-01` como base
        classlifetitulaciones_df = classlifetitulaciones_df.withColumn(column_name, coalesce(col(column_name), lit('1970-01-01').cast(column_type)))

# Mostrar el DataFrame resultante
display(classlifetitulaciones_df)

# COMMAND ----------

# DBTITLE 1,Drop duplicates
classlifetitulaciones_df = classlifetitulaciones_df.dropDuplicates()

# COMMAND ----------

# DBTITLE 1,Created view
classlifetitulaciones_df.createOrReplaceTempView("classlifetitulaciones_df_source_view")

# COMMAND ----------

# DBTITLE 1,Merge Into
# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.classlifetitulaciones AS target
# MAGIC USING classlifetitulaciones_df_source_view AS source
# MAGIC ON target.enroll_group_id = source.enroll_group_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *
