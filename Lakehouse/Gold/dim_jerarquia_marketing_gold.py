# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_JERARQUIA_MARKETING**

# COMMAND ----------

# DBTITLE 1,View temporal temp_utm_campaign
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_utm_campaign AS
# MAGIC SELECT DISTINCT 
# MAGIC     utm_campaign_id, 
# MAGIC     utm_campaign_name, 
# MAGIC     utm_strategy, 
# MAGIC     utm_channel
# MAGIC FROM silver_lakehouse.zohodeals
# MAGIC WHERE utm_campaign_id IS NOT NULL;
# MAGIC
# MAGIC SELECT * FROM temp_utm_campaign;

# COMMAND ----------

# DBTITLE 1,MERGE dim_utm_campaign
# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_utm_campaign AS target
# MAGIC USING temp_utm_campaign AS source
# MAGIC ON target.utm_campaign_id = source.utm_campaign_id
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (utm_campaign_id, utm_campaign_name, utm_strategy, utm_channel)
# MAGIC     VALUES (source.utm_campaign_id, source.utm_campaign_name, source.utm_strategy, source.utm_channel);

# COMMAND ----------

# DBTITLE 1,View temporal dim_utm_adset
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_utm_adset AS
# MAGIC SELECT DISTINCT 
# MAGIC     utm_ad_id, 
# MAGIC     utm_adset_id, 
# MAGIC     utm_term
# MAGIC FROM silver_lakehouse.zohodeals
# MAGIC WHERE utm_ad_id IS NOT NULL;
# MAGIC
# MAGIC SELECT * FROM dim_utm_adset;

# COMMAND ----------

# DBTITLE 1,MERGE dim_utm_adset
# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_utm_adset AS target
# MAGIC USING temp_utm_ad AS source
# MAGIC ON target.utm_ad_id = source.utm_ad_id
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (utm_ad_id, utm_adset_id, utm_term)
# MAGIC     VALUES (source.utm_ad_id, source.utm_adset_id, source.utm_term);

# COMMAND ----------

# DBTITLE 1,View temporal temp_utm_source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW temp_utm_source AS
# MAGIC SELECT DISTINCT 
# MAGIC     utm_source, 
# MAGIC     utm_type, 
# MAGIC     utm_medium, 
# MAGIC     utm_profile
# MAGIC FROM silver_lakehouse.zohodeals
# MAGIC WHERE utm_source IS NOT NULL;
# MAGIC
# MAGIC SELECT * FROM temp_utm_source;

# COMMAND ----------

# DBTITLE 1,MERGE dim_utm_source
# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_utm_source AS target
# MAGIC USING temp_utm_source AS source
# MAGIC ON target.utm_source = source.utm_source
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (utm_source, utm_type, utm_medium, utm_profile)
# MAGIC     VALUES (source.utm_source, source.utm_type, source.utm_medium, source.utm_profile);
