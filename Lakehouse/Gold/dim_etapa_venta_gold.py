# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW etapa_venta_sales_view
# MAGIC     AS SELECT distinct etapa_venta as nombre_etapa_venta    
# MAGIC     FROM silver_lakehouse.sales;
# MAGIC
# MAGIC select * from etapa_venta_sales_view

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_etapa_venta
# MAGIC USING etapa_venta_sales_view 
# MAGIC ON gold_lakehouse.dim_etapa_venta.nombre_etapa_venta = etapa_venta_sales_view.nombre_etapa_venta
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (gold_lakehouse.dim_etapa_venta.nombre_etapa_venta)
# MAGIC VALUES (etapa_venta_sales_view.nombre_etapa_venta)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_etapa_venta_view AS
# MAGIC SELECT DISTINCT
# MAGIC     COALESCE(zd.etapa, zl.lead_status) AS nombreEtapaVenta,
# MAGIC     CASE 
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Nuevo') THEN 'Nuevo'
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Asignado', 'Sin gestionar', 'Contactando') THEN 'Asignado'
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Contactado', 'Sin información') THEN 'Contactado'
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Seguimiento', 'Valorando', 'Cita') THEN 'Seguimiento'
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Interesado') THEN 'Interesado'
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Pendiente de pago') THEN 'Pendiente de pago'
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Pagado (NE)', 'Pendiente Entrevista', 'Pendiente prueba', 'Pendiente documentación') THEN 'Pagado (NE)'
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Matriculado') THEN 'Matriculado'
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Pagado (NEC)') THEN 'Pagado (NEC)'
# MAGIC         ELSE 'Desconocido'  -- En caso de valores no contemplados
# MAGIC     END AS nombreEtapaVentaAgrupado,
# MAGIC     CURRENT_TIMESTAMP AS ETLcreatedDate,
# MAGIC     CURRENT_TIMESTAMP AS ETLupdatedDate
# MAGIC FROM silver_lakehouse.zoholeads zl
# MAGIC FULL OUTER JOIN silver_lakehouse.zohodeals zd
# MAGIC ON zl.lead_status = zd.etapa
# MAGIC WHERE COALESCE(zd.etapa, zl.lead_status) IS NOT NULL;
# MAGIC
# MAGIC select * from dim_etapa_venta_view

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_etapa_venta AS target
# MAGIC USING dim_etapa_venta_view AS source
# MAGIC ON target.nombre_etapa_venta = source.nombreEtapaVenta
# MAGIC
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.nombreEtapaVentaAgrupado = source.nombreEtapaVentaAgrupado,
# MAGIC         target.ETLupdatedDate = source.ETLupdatedDate
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (
# MAGIC         nombre_etapa_venta,
# MAGIC         nombreEtapaVentaAgrupado,
# MAGIC         ETLcreatedDate,
# MAGIC         ETLupdatedDate
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         source.nombreEtapaVenta,
# MAGIC         source.nombreEtapaVentaAgrupado,
# MAGIC         source.ETLcreatedDate,
# MAGIC         source.ETLupdatedDate
# MAGIC     );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_lakehouse.dim_etapa_venta
