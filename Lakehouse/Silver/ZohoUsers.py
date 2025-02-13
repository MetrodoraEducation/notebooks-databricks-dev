# Databricks notebook source
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

table_name = "JsaZohoUsers"

zohousers_df = spark.read.json(f"{bronze_folder_path}/lakehouse/zoho/{current_date}/{table_name}.json")
zohousers_df

print(f"{bronze_folder_path}/lakehouse/zoho/{current_date}/{table_name}.json")

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

from pyspark.sql.types import StringType, BooleanType, LongType
from pyspark.sql.functions import col, lit, current_timestamp

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
    "default_tab_group": "default_tab_group",
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
    "name_format__s": "name_format",
    "number_separator": "number_separator",
    "offset": "offset",
    "personal_account": "personal_account",
    "phone": "phone",
    "sandboxdeveloper": "sandboxdeveloper",
    "signature": "signature",
    "sort_order_preference__s": "sort_order_preference",
    "state": "state",
    "status": "status",
    "status_reason__s": "status_reason",
    "street": "street",
    "time_format": "time_format",
    "time_zone": "time_zone",
    "type__s": "type",
    "website": "website",
    "zip": "zip",
    "zuid": "zuid",
    "modified_by_id": "modified_by_id",
    "modified_by_name": "modified_by_name",
    "created_by_id": "created_by_id",
    "created_by_name": "created_by_name",
    "profile_id": "profile_id",
    "profile_name": "profile_name",
    "role_id": "role_id",
    "role_name": "role_name",
}

# Renombrar columnas dinámicamente
for old_col, new_col in columns_mapping.items():
    if old_col in zohousers_df.columns:
        zohousers_df = zohousers_df.withColumnRenamed(old_col, new_col)

# Seleccionar solo las columnas necesarias después de renombrarlas
selected_columns = list(columns_mapping.values())

# Filtrar el DataFrame solo con las columnas deseadas
zohousers_df = zohousers_df.select(*selected_columns)

# Agregar columnas adicionales (si es necesario)
zohousers_df = zohousers_df \
    .withColumn("processdate", current_timestamp()) \
    .withColumn("sourcesystem", lit("zoho_Users"))

# Mostrar el DataFrame resultante
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
    .withColumn("default_tab_group", col("default_tab_group").cast(StringType())) \
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
    .withColumn("name_format", col("name_format").cast(StringType())) \
    .withColumn("number_separator", col("number_separator").cast(StringType())) \
    .withColumn("offset", col("offset").cast(LongType())) \
    .withColumn("personal_account", col("personal_account").cast(BooleanType())) \
    .withColumn("phone", col("phone").cast(StringType())) \
    .withColumn("sandboxdeveloper", col("sandboxdeveloper").cast(BooleanType())) \
    .withColumn("signature", col("signature").cast(StringType())) \
    .withColumn("sort_order_preference", col("sort_order_preference").cast(StringType())) \
    .withColumn("state", col("state").cast(StringType())) \
    .withColumn("status", col("status").cast(StringType())) \
    .withColumn("status_reason", col("status_reason").cast(StringType())) \
    .withColumn("street", col("street").cast(StringType())) \
    .withColumn("time_format", col("time_format").cast(StringType())) \
    .withColumn("time_zone", col("time_zone").cast(StringType())) \
    .withColumn("type", col("type").cast(StringType())) \
    .withColumn("website", col("website").cast(StringType())) \
    .withColumn("zip", col("zip").cast(StringType())) \
    .withColumn("zuid", col("zuid").cast(StringType())) \
    .withColumn("modified_by_id", col("modified_by_id").cast(StringType())) \
    .withColumn("modified_by_name", col("modified_by_name").cast(StringType())) \
    .withColumn("created_by_id", col("created_by_id").cast(StringType())) \
    .withColumn("created_by_name", col("created_by_name").cast(StringType())) \
    .withColumn("profile_id", col("profile_id").cast(StringType())) \
    .withColumn("profile_name", col("profile_name").cast(StringType())) \
    .withColumn("role_id", col("role_id").cast(StringType())) \
    .withColumn("role_name", col("role_name").cast(StringType()))

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
