# Databricks notebook source
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

table_name = "JsaZohoUsers"

zohousers_df = spark.read.json(f"{bronze_folder_path}/lakehouse/zoho/{current_date}/{table_name}.json")
zohousers_df

# COMMAND ----------

zohousers_df = zohousers_df.select("users")

# COMMAND ----------

zohousers_df = flatten(zohousers_df)

# COMMAND ----------

# Imprime las columnas disponibles antes de procesarlas
print("Columnas disponibles en el DataFrame:")
print(zohousers_df.columns)

# Renombra columnas, asegurándote de que las columnas existen
for col_name in zohousers_df.columns:
    new_col_name = col_name.replace("users_$", "").replace("users_", "")
    zohousers_df = zohousers_df.withColumnRenamed(col_name, new_col_name)

# Muestra el DataFrame procesado
display(zohousers_df)

# COMMAND ----------

# DBTITLE 1,Columnas a minusculas
for col in zohousers_df.columns:
    zohousers_df = zohousers_df.withColumnRenamed(col, col.lower())

# COMMAND ----------

# Diccionario para mapear las columnas directamente, manteniendo los nombres originales
columns_mapping = {
    "current_shift": "current_shift",
    "next_shift": "next_shift",
    "shift_effective_from": "shift_effective_from",
    "currency": "currency",
    "isonline": "isonline",
    "modified_time": "modified_time",
    "alias": "alias",
    "city": "city",
    "confirm": "confirm",
    "country": "country",
    "country_locale": "country_locale",
    "created_time": "created_time",
    "date_format": "date_format",
    "decimal_separator": "decimal_separator",
    "dob": "dob",
    "email": "email",
    "fax": "fax",
    "first_name": "first_name",
    "full_name": "full_name",
    "id": "id",
    "language": "language",
    "last_name": "last_name",
    "locale": "locale",
    "microsoft": "microsoft",
    "mobile": "mobile",
    "number_separator": "number_separator",
    "offset": "offset",
    "phone": "phone",
    "sandboxdeveloper": "sandboxdeveloper",
    "state": "state",
    "status_reason__s": "status_reason__s",
    "street": "street",
    "time_format": "time_format",
    "time_zone": "time_zone",
    "website": "website",
    "zip": "zip",
    "zuid": "zuid",
    "modified_by_id": "modified_by_id",
    "modified_by_name": "modified_by_name",
    "created_by_id": "created_by_id",
    "created_by_name": "created_by_name"
}

# Renombrar columnas dinámicamente (en este caso, no cambiarán porque el mapeo es 1 a 1)
for old_col, new_col in columns_mapping.items():
    if old_col in zohousers_df.columns:
        zohousers_df = zohousers_df.withColumnRenamed(old_col, new_col)

# Mostrar el DataFrame resultante
display(zohousers_df)

# COMMAND ----------

display(zohousers_df)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# Ajuste del DataFrame con validación de columnas para zohousers
zohousers_df = zohousers_df \
    .withColumn("processdate", current_timestamp()) \
    .withColumn("sourcesystem", lit("zoho_Users")) \
    .withColumn("current_shift", col("current_shift").cast(StringType())) \
    .withColumn("next_shift", col("next_shift").cast(StringType())) \
    .withColumn("shift_effective_from", to_timestamp(col("shift_effective_from"), "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    .withColumn("currency", col("currency").cast(StringType())) \
    .withColumn("isonline", col("isonline").cast(BooleanType())) \
    .withColumn("modified_time", to_timestamp(col("modified_time"), "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    .withColumn("alias", col("alias").cast(StringType())) \
    .withColumn("city", col("city").cast(StringType())) \
    .withColumn("confirm", col("confirm").cast(BooleanType())) \
    .withColumn("country", col("country").cast(StringType())) \
    .withColumn("country_locale", col("country_locale").cast(StringType())) \
    .withColumn("created_time", to_timestamp(col("created_time"), "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    .withColumn("date_format", col("date_format").cast(StringType())) \
    .withColumn("decimal_separator", col("decimal_separator").cast(StringType())) \
    .withColumn("dob", to_date(col("dob"), "yyyy-MM-dd")) \
    .withColumn("email", col("email").cast(StringType())) \
    .withColumn("fax", col("fax").cast(StringType())) \
    .withColumn("first_name", col("first_name").cast(StringType())) \
    .withColumn("full_name", col("full_name").cast(StringType())) \
    .withColumn("id", col("id").cast(StringType())) \
    .withColumn("language", col("language").cast(StringType())) \
    .withColumn("last_name", col("last_name").cast(StringType())) \
    .withColumn("locale", col("locale").cast(StringType())) \
    .withColumn("microsoft", col("microsoft").cast(BooleanType())) \
    .withColumn("mobile", col("mobile").cast(StringType())) \
    .withColumn("number_separator", col("number_separator").cast(StringType())) \
    .withColumn("offset", col("offset").cast(LongType())) \
    .withColumn("phone", col("phone").cast(StringType())) \
    .withColumn("sandboxdeveloper", col("sandboxdeveloper").cast(BooleanType())) \
    .withColumn("state", col("state").cast(StringType())) \
    .withColumn("status_reason__s", col("status_reason__s").cast(StringType())) \
    .withColumn("street", col("street").cast(StringType())) \
    .withColumn("time_format", col("time_format").cast(StringType())) \
    .withColumn("time_zone", col("time_zone").cast(StringType())) \
    .withColumn("website", col("website").cast(StringType())) \
    .withColumn("zip", col("zip").cast(StringType())) \
    .withColumn("zuid", col("zuid").cast(StringType())) \
    .withColumn("modified_by_id", col("modified_by_id").cast(StringType())) \
    .withColumn("modified_by_name", col("modified_by_name").cast(StringType())) \
    .withColumn("created_by_id", col("created_by_id").cast(StringType())) \
    .withColumn("created_by_name", col("created_by_name").cast(StringType()))

# Mostrar el DataFrame final
display(zohousers_df)


# COMMAND ----------

from pyspark.sql.functions import coalesce, lit, col

# Reemplaza valores nulos en columnas basadas en sus tipos de datos para zohousers_df
for t in zohousers_df.dtypes:
    column_name = t[0]
    column_type = t[1]
    
    if column_type == 'string':
        zohousers_df = zohousers_df.withColumn(column_name, coalesce(col(column_name), lit('')))
    elif column_type in ['double', 'float']:
        zohousers_df = zohousers_df.withColumn(column_name, coalesce(col(column_name), lit(0.0)))
    elif column_type in ['int', 'bigint']:
        zohousers_df = zohousers_df.withColumn(column_name, coalesce(col(column_name), lit(0)))
    elif column_type == 'boolean':
        zohousers_df = zohousers_df.withColumn(column_name, coalesce(col(column_name), lit(False)))
    elif column_type in ['timestamp', 'date']:
        # Para fechas y timestamps dejamos `None` explícitamente
        zohousers_df = zohousers_df.withColumn(column_name, coalesce(col(column_name), lit(None)))

# Mostrar el DataFrame resultante
display(zohousers_df)

# COMMAND ----------

zohousers_df = zohousers_df.dropDuplicates()

# COMMAND ----------

zohousers_df.createOrReplaceTempView("zohousers_source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.zohousers
# MAGIC USING zohousers_source_view
# MAGIC ON silver_lakehouse.zohousers.id = zohousers_source_view.id
# MAGIC    --AND silver_lakehouse.zohousers.email = zohousers_source_view.email
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
