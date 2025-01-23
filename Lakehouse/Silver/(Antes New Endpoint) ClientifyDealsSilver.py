# Databricks notebook source
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

#clientify_df = spark.read.json(f"{bronze_folder_path}/lakehouse/clientify/deals/{current_date}")
clientify_df = spark.read.json(f"{bronze_folder_path}/lakehouse/clientify/deals/2024/10/31")

# COMMAND ----------

clientify_df = clientify_df.select("results")

# COMMAND ----------

clientify_df = flatten(clientify_df)

# COMMAND ----------

clientify_df_result = clientify_df.drop("results_tags","results_custom_fields_field","results_custom_fields_id","results_custom_fields_value")

# COMMAND ----------

clientify_df_result = clientify_df_result.dropDuplicates()

# COMMAND ----------

clientify_df_custom_fields = clientify_df.select("results_id","results_custom_fields_field","results_custom_fields_id","results_custom_fields_value")

# COMMAND ----------

clientify_df_custom_fields = clientify_df_custom_fields.dropDuplicates()

# COMMAND ----------

clientify_df_custom_fields = clientify_df_custom_fields.filter(clientify_df_custom_fields.results_custom_fields_field != "Año Académico")

# COMMAND ----------

# MAGIC %md
# MAGIC Problema nombre columna custum_field, haciendo pivot sale solo el valor en el nombre de la columna, luego pasa que tenemos el id de result y el id de custumfield

# COMMAND ----------

clientify_df_custom_fields = clientify_df_custom_fields.withColumn(
    "results_custom_fields_field",
    concat(lit("custom_fields_"), clientify_df_custom_fields["results_custom_fields_field"])
)

# COMMAND ----------

clientify_df_custom_fields = clientify_df_custom_fields.groupBy("results_id").pivot("results_custom_fields_field").agg(first(col("results_custom_fields_value")))

#first("results_custom_fields_value")

# COMMAND ----------

for col in clientify_df_custom_fields.columns:
    clientify_df_custom_fields = clientify_df_custom_fields.withColumnRenamed(col, col.replace(" ", "_"))

# COMMAND ----------

for col in clientify_df_custom_fields.columns:
    clientify_df_custom_fields = clientify_df_custom_fields.withColumnRenamed(col, col.lower())

# COMMAND ----------

for col in clientify_df_custom_fields.columns:
    clientify_df_custom_fields = clientify_df_custom_fields.withColumnRenamed(col, col.replace("-", "_"))

# COMMAND ----------

clientify_df_custom_fields = clientify_df_custom_fields.withColumnRenamed("results_id","results_id_custom_fields")

# COMMAND ----------

clientify_df_final = clientify_df_result.join(
    clientify_df_custom_fields, 
    on=clientify_df_result.results_id == clientify_df_custom_fields.results_id_custom_fields, 
    how='outer'
)

# COMMAND ----------

clientify_df_final = clientify_df_final.drop("results_id_custom_fields")

# COMMAND ----------

for col in clientify_df_final.columns:
    clientify_df_final = clientify_df_final.withColumnRenamed(col, col.replace("results_", ""))

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
clientify_df_final = clientify_df_final \
    .withColumn("processdate", current_timestamp()) \
    .withColumn("sourcesystem", lit("Clientify")) \
    .withColumn("id", col("id").cast(StringType())) \
    .withColumn("probability", col("probability").cast(IntegerType())) \
    .withColumn("status", col("status").cast(IntegerType())) \
    .withColumn("amount", col("amount").cast(DoubleType())) \
    .withColumn("custom_fields_descuento", col("custom_fields_descuento").cast(DoubleType())) \
    .withColumn("custom_fields_descuento_matricula", col("custom_fields_descuento_matricula").cast(DoubleType())) \
    .withColumn("custom_fields_matricula", regexp_replace(
        regexp_replace(col("custom_fields_matricula"), "\.", ""), ",", ".").cast(DoubleType())) \
    .withColumn("custom_fields_mensualidad", regexp_replace(
        regexp_replace(col("custom_fields_mensualidad"), "\.", ""), ",", ".").cast(DoubleType())) \
    .withColumn("custom_fields_byratings_score", col("custom_fields_byratings_score").cast(DoubleType())) \
    .withColumn("actual_closed_date", to_timestamp(col("actual_closed_date"))) \
    .withColumn("created", to_timestamp(col("created"))) \
    .withColumn("expected_closed_date", to_timestamp(col("expected_closed_date"))) \
    .withColumn("modified", to_timestamp(col("modified"))) \
    .withColumn("custom_fields_fecha_inscripcion", to_timestamp(col("custom_fields_fecha_inscripcion"))) \
    .withColumn("probability_desc", regexp_replace(col("probability_desc"), "%", "").cast(DoubleType()))
#    .withColumn("date_action_last", to_timestamp(col("date_action_last"), 'yyyy-MM-dd HH:mm:ss')) \

# COMMAND ----------

clientify_df_final.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE WITH SCHEMA EVOLUTION INTO silver_lakehouse.clientifydeals
# MAGIC USING source_view 
# MAGIC ON silver_lakehouse.clientifydeals.id = source_view.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

clientify_df_final.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from silver_lakehouse.clientifydeals limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC aqui perdemos la franja horaria por ejemplo para created,  problema coma en lugar de cero custom_fields_matricula, custom_fields_mensualidad

# COMMAND ----------

#clientify_df_final.write \
#    .mode("overwrite") \
#    .option("mergeSchema", "true") \
#    .format("delta") \
#    .saveAsTable("silver_lakehouse.clientifydeals")

# COMMAND ----------

# MAGIC %sql
# MAGIC --select count(*) from silver_lakehouse.clientifydeals 
