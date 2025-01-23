# Databricks notebook source
# MAGIC %md
# MAGIC DimMotivoPerdidaLlamada

# COMMAND ----------

MotivoPerdidaLlamada_df = spark.sql("select distinct ifnull(missed_call_reason,'' ) as missed_call_reason from silver_lakehouse.aircallcalls ")

# COMMAND ----------

MotivoPerdidaLlamada_df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insertar los datos transformados en fct_llamada sin duplicados
# MAGIC MERGE INTO gold_lakehouse.dim_motivo_perdida_llamada AS target
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         missed_call_reason AS motivo_perdida_llamada,
# MAGIC         CASE 
# MAGIC             WHEN missed_call_reason IN ('abandoned_in_ivr', 'abandoned_in_classic', 'short_abandoned') THEN 'Abandono'
# MAGIC             WHEN missed_call_reason IN ('no_available_agent', 'agents_did_not_answer') THEN 'Comercial'
# MAGIC             ELSE 'n/a' --n/a
# MAGIC         END AS tipo_perdida
# MAGIC     FROM source_view
# MAGIC ) AS source
# MAGIC ON target.motivo_perdida_llamada = source.motivo_perdida_llamada -- Condición para identificar registros existentes
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (motivo_perdida_llamada, tipo_perdida)
# MAGIC VALUES (source.motivo_perdida_llamada, source.tipo_perdida);
