# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_MOTIVO_PERDIDA**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_motivo_perdida_clientify_view AS
# MAGIC SELECT DISTINCT
# MAGIC     TRIM(lost_reason) AS nombreDimMotivoPerdida,
# MAGIC     CURRENT_TIMESTAMP AS ETLcreatedDate,
# MAGIC     CURRENT_TIMESTAMP AS ETLupdatedDate
# MAGIC FROM silver_lakehouse.clientifydeals
# MAGIC WHERE lost_reason IS NOT NULL AND lost_reason <> '';
# MAGIC
# MAGIC select * from dim_motivo_perdida_clientify_view

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_motivo_perdida AS target
# MAGIC USING dim_motivo_perdida_clientify_view AS source
# MAGIC ON target.nombreDimMotivoPerdida = source.nombreDimMotivoPerdida
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.ETLupdatedDate = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombreDimMotivoPerdida, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombreDimMotivoPerdida, source.ETLcreatedDate, source.ETLupdatedDate);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_motivo_perdida_view AS
# MAGIC SELECT DISTINCT
# MAGIC     COALESCE(zd.motivo_perdida_b2c, zd.motivo_perdida_b2b, zl.motivos_perdida) AS nombreDimMotivoPerdida,
# MAGIC     CURRENT_TIMESTAMP AS ETLcreatedDate,
# MAGIC     CURRENT_TIMESTAMP AS ETLupdatedDate
# MAGIC FROM silver_lakehouse.zoholeads zl
# MAGIC FULL OUTER JOIN silver_lakehouse.zohodeals zd
# MAGIC ON zl.id = zd.id  -- Asegurar la clave de unión (puede cambiar según los datos)
# MAGIC WHERE COALESCE(zd.motivo_perdida_b2c, zd.motivo_perdida_b2b, zl.motivos_perdida) IS NOT NULL;
# MAGIC
# MAGIC select * from dim_motivo_perdida_view

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_motivo_perdida AS target
# MAGIC USING dim_motivo_perdida_view AS source
# MAGIC ON target.nombreDimMotivoPerdida = source.nombreDimMotivoPerdida
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.ETLupdatedDate = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombreDimMotivoPerdida, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombreDimMotivoPerdida, source.ETLcreatedDate, source.ETLupdatedDate);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_lakehouse.dim_motivo_perdida
