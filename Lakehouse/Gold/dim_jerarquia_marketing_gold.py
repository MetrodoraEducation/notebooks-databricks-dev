# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_JERARQUIA_MARKETING**

# COMMAND ----------

# DBTITLE 1,View temporal temp_utm_campaign
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_utm_campaign AS
# MAGIC WITH ranked_utm_campaign AS (
# MAGIC     SELECT 
# MAGIC         utm_campaign_id, 
# MAGIC         utm_campaign_name, 
# MAGIC         utm_strategy, 
# MAGIC         utm_channel,
# MAGIC         processdate,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY utm_campaign_id 
# MAGIC             ORDER BY processdate DESC  -- Se queda con la versión más reciente
# MAGIC         ) AS row_num,
# MAGIC         MIN(processdate) OVER (PARTITION BY utm_campaign_id) AS ETLcreatedDate,
# MAGIC         MAX(processdate) OVER (PARTITION BY utm_campaign_id) AS ETLupdatedDate
# MAGIC     FROM (
# MAGIC         SELECT 
# MAGIC             utm_campaign_id, utm_campaign_name, utm_strategy, utm_channel, processdate 
# MAGIC         FROM silver_lakehouse.zohodeals
# MAGIC         WHERE utm_campaign_id IS NOT NULL
# MAGIC         
# MAGIC         UNION ALL
# MAGIC         
# MAGIC         SELECT 
# MAGIC             utm_campaign_id, utm_campaign_name, utm_strategy, utm_channel, processdate 
# MAGIC         FROM silver_lakehouse.zoholeads
# MAGIC         WHERE utm_campaign_id IS NOT NULL
# MAGIC     ) utm_campaign_data
# MAGIC )
# MAGIC SELECT 
# MAGIC     utm_campaign_id, 
# MAGIC     utm_campaign_name, 
# MAGIC     utm_strategy, 
# MAGIC     utm_channel,
# MAGIC     ETLcreatedDate,
# MAGIC     ETLupdatedDate
# MAGIC FROM ranked_utm_campaign
# MAGIC WHERE row_num = 1; -- Se queda solo con la fila más reciente por utm_campaign_id
# MAGIC
# MAGIC SELECT * FROM temp_utm_campaign;

# COMMAND ----------

# DBTITLE 1,MERGE dim_utm_campaign
# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_utm_campaign AS target
# MAGIC USING temp_utm_campaign AS source
# MAGIC ON target.utm_campaign_id = source.utm_campaign_id
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     COALESCE(target.utm_campaign_name, '') <> COALESCE(source.utm_campaign_name, '') OR
# MAGIC     COALESCE(target.utm_strategy, '') <> COALESCE(source.utm_strategy, '') OR
# MAGIC     COALESCE(target.utm_channel, '') <> COALESCE(source.utm_channel, '') OR
# MAGIC     target.ETLupdatedDate < source.ETLupdatedDate  -- Solo actualizar si hay una versión más reciente
# MAGIC )
# MAGIC THEN UPDATE SET 
# MAGIC     target.utm_campaign_name = source.utm_campaign_name,
# MAGIC     target.utm_strategy = source.utm_strategy,
# MAGIC     target.utm_channel = source.utm_channel,
# MAGIC     target.ETLupdatedDate = source.ETLupdatedDate
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (utm_campaign_id, utm_campaign_name, utm_strategy, utm_channel, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.utm_campaign_id, source.utm_campaign_name, source.utm_strategy, source.utm_channel, source.ETLcreatedDate, source.ETLupdatedDate);

# COMMAND ----------

# DBTITLE 1,View temporal dim_utm_adset
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_utm_adset AS
# MAGIC WITH ranked_utm_adset AS (
# MAGIC     SELECT 
# MAGIC         utm_ad_id, 
# MAGIC         utm_adset_id, 
# MAGIC         utm_term,
# MAGIC         processdate,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY utm_ad_id 
# MAGIC             ORDER BY processdate DESC  -- Mantiene la versión más reciente
# MAGIC         ) AS row_num,
# MAGIC         MIN(processdate) OVER (PARTITION BY utm_ad_id) AS ETLcreatedDate,
# MAGIC         MAX(processdate) OVER (PARTITION BY utm_ad_id) AS ETLupdatedDate
# MAGIC     FROM (
# MAGIC         SELECT 
# MAGIC             utm_ad_id, utm_adset_id, utm_term, processdate 
# MAGIC         FROM silver_lakehouse.zohodeals
# MAGIC         WHERE utm_ad_id IS NOT NULL
# MAGIC         UNION ALL
# MAGIC         SELECT 
# MAGIC             utm_ad_id, utm_adset_id, utm_term, processdate 
# MAGIC         FROM silver_lakehouse.zoholeads
# MAGIC         WHERE utm_ad_id IS NOT NULL
# MAGIC     ) utm_ad_data
# MAGIC )
# MAGIC SELECT 
# MAGIC     utm_ad_id, 
# MAGIC     utm_adset_id, 
# MAGIC     utm_term,
# MAGIC     ETLcreatedDate,
# MAGIC     ETLupdatedDate
# MAGIC FROM ranked_utm_adset
# MAGIC WHERE row_num = 1; -- Se queda solo con la fila más reciente por utm_ad_id
# MAGIC
# MAGIC SELECT * FROM temp_utm_adset;
# MAGIC

# COMMAND ----------

# DBTITLE 1,MERGE dim_utm_adset
# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_utm_adset AS target
# MAGIC USING temp_utm_adset AS source
# MAGIC ON target.utm_ad_id = source.utm_ad_id
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     COALESCE(target.utm_adset_id, '') <> COALESCE(source.utm_adset_id, '') OR
# MAGIC     COALESCE(target.utm_term, '') <> COALESCE(source.utm_term, '') OR
# MAGIC     target.ETLupdatedDate < source.ETLupdatedDate  -- Solo actualizar si hay una versión más reciente
# MAGIC )
# MAGIC THEN UPDATE SET 
# MAGIC     target.utm_adset_id = source.utm_adset_id,
# MAGIC     target.utm_term = source.utm_term,
# MAGIC     target.ETLupdatedDate = source.ETLupdatedDate
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (utm_ad_id, utm_adset_id, utm_term, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.utm_ad_id, source.utm_adset_id, source.utm_term, source.ETLcreatedDate, source.ETLupdatedDate);
# MAGIC

# COMMAND ----------

# DBTITLE 1,View temporal temp_utm_source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_utm_source AS
# MAGIC WITH ranked_utm_source AS (
# MAGIC     SELECT 
# MAGIC         utm_source, 
# MAGIC         utm_type, 
# MAGIC         utm_medium, 
# MAGIC         utm_profile,
# MAGIC         processdate,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY utm_source 
# MAGIC             ORDER BY processdate DESC  -- Mantiene la versión más reciente
# MAGIC         ) AS row_num,
# MAGIC         MIN(processdate) OVER (PARTITION BY utm_source) AS ETLcreatedDate,
# MAGIC         MAX(processdate) OVER (PARTITION BY utm_source) AS ETLupdatedDate
# MAGIC     FROM (
# MAGIC         SELECT 
# MAGIC             utm_source, utm_type, utm_medium, utm_profile, processdate 
# MAGIC         FROM silver_lakehouse.zohodeals
# MAGIC         WHERE utm_source IS NOT NULL
# MAGIC         UNION ALL
# MAGIC         SELECT 
# MAGIC             utm_source, utm_type, utm_medium, utm_profile, processdate 
# MAGIC         FROM silver_lakehouse.zoholeads
# MAGIC         WHERE utm_source IS NOT NULL
# MAGIC     ) utm_source_data
# MAGIC )
# MAGIC SELECT 
# MAGIC     utm_source, 
# MAGIC     utm_type, 
# MAGIC     utm_medium, 
# MAGIC     utm_profile,
# MAGIC     ETLcreatedDate,
# MAGIC     ETLupdatedDate
# MAGIC FROM ranked_utm_source
# MAGIC WHERE row_num = 1; -- Se queda solo con la fila más reciente por utm_source
# MAGIC
# MAGIC SELECT * FROM temp_utm_source;
# MAGIC

# COMMAND ----------

# DBTITLE 1,MERGE dim_utm_source
# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_utm_source AS target
# MAGIC USING temp_utm_source AS source
# MAGIC ON target.utm_source = source.utm_source
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     COALESCE(target.utm_type, '') <> COALESCE(source.utm_type, '') OR
# MAGIC     COALESCE(target.utm_medium, '') <> COALESCE(source.utm_medium, '') OR
# MAGIC     COALESCE(target.utm_profile, '') <> COALESCE(source.utm_profile, '') OR
# MAGIC     target.ETLupdatedDate < source.ETLupdatedDate  -- Solo actualizar si hay una versión más reciente
# MAGIC )
# MAGIC THEN UPDATE SET 
# MAGIC     target.utm_type = source.utm_type,
# MAGIC     target.utm_medium = source.utm_medium,
# MAGIC     target.utm_profile = source.utm_profile,
# MAGIC     target.ETLupdatedDate = source.ETLupdatedDate
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (utm_source, utm_type, utm_medium, utm_profile, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.utm_source, source.utm_type, source.utm_medium, source.utm_profile, source.ETLcreatedDate, source.ETLupdatedDate);
