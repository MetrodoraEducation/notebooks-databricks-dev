# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_CONCEPTO_COBRO**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_concepto_cobro_view AS
# MAGIC     SELECT DISTINCT
# MAGIC         receipts.receipt_concept AS concepto
# MAGIC         ,CASE WHEN receipt_concept LIKE 'Certificado%' THEN 0
# MAGIC               ELSE 1
# MAGIC          END tipo_reparto
# MAGIC     FROM silver_lakehouse.ClasslifeReceipts receipts;
# MAGIC
# MAGIC select * from dim_concepto_cobro_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ 🔹 Asegurar que el registro `id_dim_concepto_cobro = -1` existe solo una vez con valores `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_concepto_cobro AS target
# MAGIC USING (
# MAGIC     SELECT 'n/a' AS concepto, 'n/a' AS tipo_reparto
# MAGIC ) AS source
# MAGIC ON UPPER(target.concepto) = 'n/a'
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (concepto, tipo_reparto, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES ('n/a', 'n/a', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- 2️⃣ 🔹 Insertar valores predeterminados si no existen en `dim_concepto_cobro`
# MAGIC MERGE INTO gold_lakehouse.dim_concepto_cobro AS target
# MAGIC USING (
# MAGIC     SELECT 'Matrícula' AS concepto, '1' AS tipo_reparto UNION ALL
# MAGIC     SELECT 'Docencia', '1' UNION ALL
# MAGIC     SELECT 'Certificado', '0'
# MAGIC ) AS source
# MAGIC ON UPPER(target.concepto) = UPPER(source.concepto)
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (concepto, tipo_reparto, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.concepto, source.tipo_reparto, current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- 3️⃣ 🔹 Insertar nuevos valores desde `dim_concepto_cobro_view` sin alterar el registro `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_concepto_cobro AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT concepto, tipo_reparto FROM dim_concepto_cobro_view WHERE concepto <> 'n/a'
# MAGIC ) AS source
# MAGIC ON UPPER(target.concepto) = UPPER(source.concepto)
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     COALESCE(TRIM(UPPER(target.tipo_reparto)), '') <> COALESCE(TRIM(UPPER(source.tipo_reparto)), '')
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     target.tipo_reparto = source.tipo_reparto,
# MAGIC     target.ETLupdatedDate = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (concepto, tipo_reparto, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (
# MAGIC         source.concepto,
# MAGIC         source.tipo_reparto,
# MAGIC         current_timestamp(), 
# MAGIC         current_timestamp()
# MAGIC     );
# MAGIC
