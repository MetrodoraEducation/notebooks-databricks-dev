# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_COMERCIAL**

# COMMAND ----------

# MAGIC %md
# MAGIC  Explicación Detallada del Código
# MAGIC ✅ Vista Temporal (dim_comercial_temp_view):
# MAGIC
# MAGIC Extrae los datos desde zohousers y los prepara para la actualización.
# MAGIC Establece fechaDesde = current_date(), mientras que fechaHasta queda NULL.
# MAGIC ✅ MERGE con dim_comercial:
# MAGIC
# MAGIC Si el equipoComercial cambia, se cierra el registro anterior (fechaHasta = current_date()) y se inserta un nuevo registro con fechaDesde = current_date().
# MAGIC Si el codComercial no existe, se inserta directamente.
# MAGIC ✅ Cálculo de esActivo:
# MAGIC
# MAGIC Un comercial es activo (1) si no tiene fechaHasta o si fechaDesde ≤ current_date < fechaHasta.
# MAGIC Es inactivo (0) si su fechaHasta ya ha pasado.

# COMMAND ----------

# MAGIC %md
# MAGIC **Gestion Calls**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW comercial_sales_view
# MAGIC     AS SELECT DISTINCT 
# MAGIC     propietario_lead AS nombre_comercial,
# MAGIC     institucion AS equipo_comercial,
# MAGIC     '' AS cod_comercial,  -- Si no tienes esta columna en la fuente, se asigna un valor vacío
# MAGIC     TRUE AS activo,       -- Si no tienes la columna, puedes definirla como TRUE o algún valor por defecto
# MAGIC     CURRENT_DATE() AS fecha_desde  -- Si no tienes la columna, usa la fecha actual como predeterminado
# MAGIC FROM silver_lakehouse.sales
# MAGIC WHERE propietario_lead <> 'n/a';

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_comercial AS target
# MAGIC USING comercial_sales_view AS source
# MAGIC ON UPPER(target.nombre_comercial) = UPPER(source.nombre_comercial)
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.equipo_comercial = source.equipo_comercial,
# MAGIC         target.cod_comercial = source.cod_comercial,
# MAGIC         target.fecha_hasta = CASE 
# MAGIC                                 WHEN target.equipo_comercial <> source.equipo_comercial 
# MAGIC                                   OR target.cod_comercial <> source.cod_comercial 
# MAGIC                                 THEN CURRENT_DATE() 
# MAGIC                                 ELSE target.fecha_hasta 
# MAGIC                              END
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_comercial, equipo_comercial, cod_comercial, activo, fecha_desde, fecha_hasta)
# MAGIC     VALUES (source.nombre_comercial, source.equipo_comercial, source.cod_comercial, source.activo, source.fecha_desde, NULL);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW comercial_calls_view
# MAGIC     AS SELECT distinct user_name from silver_lakehouse.aircallcalls where user_name<>'n/a'

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_comercial
# MAGIC USING comercial_calls_view 
# MAGIC ON gold_lakehouse.dim_comercial.nombre_comercial = comercial_calls_view.user_name
# MAGIC --WHEN MATCHED THEN UPDATE SET gold_lakehouse.dim_comercial.equipo_comercial = comercial_sales_view.equipo_comercial
# MAGIC WHEN NOT MATCHED THEN INSERT (gold_lakehouse.dim_comercial.nombre_comercial, gold_lakehouse.dim_comercial.equipo_comercial, gold_lakehouse.dim_comercial.activo, gold_lakehouse.dim_comercial.fecha_desde)
# MAGIC VALUES (comercial_calls_view.user_name, 'n/a', -1, current_date())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_lakehouse.dim_comercial

# COMMAND ----------

# MAGIC %md
# MAGIC **Gestion Zoho**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW dim_comercial_temp_view AS
# MAGIC SELECT
# MAGIC     z.id AS cod_Comercial,
# MAGIC     z.full_name AS nombre_Comercial,
# MAGIC     z.role_name AS equipo_Comercial,
# MAGIC     current_date() AS fecha_Desde,
# MAGIC     NULL AS fecha_Hasta
# MAGIC FROM silver_lakehouse.zohousers z;
# MAGIC
# MAGIC select * from dim_comercial_temp_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_comercial AS target
# MAGIC USING dim_comercial_temp_view AS source
# MAGIC ON target.cod_Comercial = source.cod_Comercial
# MAGIC WHEN MATCHED AND target.equipo_Comercial <> source.equipo_Comercial THEN
# MAGIC     -- Cerramos el registro anterior si cambia el equipo comercial
# MAGIC     UPDATE SET target.fecha_Hasta = current_date()
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     -- Insertamos un nuevo comercial si no existe en la dimensión
# MAGIC     INSERT (cod_Comercial, nombre_Comercial, equipo_Comercial, fecha_Desde, fecha_Hasta)
# MAGIC     VALUES (source.cod_Comercial, source.nombre_Comercial, source.equipo_Comercial, source.fecha_Desde, source.fecha_Hasta);

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE gold_lakehouse.dim_comercial
# MAGIC SET Activo = CASE 
# MAGIC     WHEN fecha_Hasta IS NULL OR current_date() BETWEEN fecha_Desde AND fecha_Hasta THEN 1
# MAGIC     ELSE 0
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT nombre_comercial, COUNT(*)
# MAGIC FROM dim_comercial_temp_view
# MAGIC GROUP BY nombre_comercial
# MAGIC HAVING COUNT(*) > 1;
