# Databricks notebook source
# DBTITLE 1,View: lead_opportunity_mapping_view
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW lead_opportunity_mapping_view AS     
# MAGIC     SELECT DISTINCT
# MAGIC         leads.id AS lead_id,
# MAGIC         deals.id AS opportunity_id,
# MAGIC         current_timestamp() AS mapping_date
# MAGIC     FROM silver_lakehouse.zoholeads leads
# MAGIC     INNER JOIN silver_lakehouse.zohodeals deals
# MAGIC         ON leads.id_correlacion_prospecto = deals.id_prospecto;

# COMMAND ----------

# DBTITLE 1,lead_opportunity_mapping
# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.lead_opportunity_mapping AS target
# MAGIC USING lead_opportunity_mapping_view AS source
# MAGIC ON target.lead_id = source.lead_id AND target.opportunity_id = source.opportunity_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET target.mapping_date = source.mapping_date
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (lead_id, opportunity_id, mapping_date)
# MAGIC   VALUES (source.lead_id, source.opportunity_id, source.mapping_date);

# COMMAND ----------

# DBTITLE 1,View: lead_contact_mapping_view
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW lead_contact_mapping_view AS     
# MAGIC     SELECT DISTINCT
# MAGIC         leads.id AS lead_id,
# MAGIC         contacts.id AS contact_id,
# MAGIC         current_timestamp() AS mapping_date
# MAGIC     FROM silver_lakehouse.zoholeads leads
# MAGIC     INNER JOIN silver_lakehouse.zohocontacts contacts
# MAGIC         ON leads.email = contacts.email;

# COMMAND ----------

# DBTITLE 1,lead_contact_mapping
# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.lead_contact_mapping AS target
# MAGIC USING lead_contact_mapping_view AS source
# MAGIC ON target.lead_id = source.lead_id AND target.contact_id = source.contact_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET target.mapping_date = source.mapping_date
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (lead_id, contact_id, mapping_date)
# MAGIC   VALUES (source.lead_id, source.contact_id, source.mapping_date);

# COMMAND ----------

# DBTITLE 1,View: campaign_lead_mapping_view
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW campaign_lead_mapping_view AS     
# MAGIC     SELECT DISTINCT
# MAGIC         campaigns.id AS campaign_id,
# MAGIC         leads.id AS lead_id,
# MAGIC         current_timestamp() AS mapping_date
# MAGIC     FROM silver_lakehouse.zoholeads leads
# MAGIC     INNER JOIN silver_lakehouse.zohocampaigns campaigns
# MAGIC         ON leads.utm_campaign_id = campaigns.id;

# COMMAND ----------

# DBTITLE 1,campaign_lead_mapping
# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.campaign_lead_mapping AS target
# MAGIC USING campaign_lead_mapping_view AS source
# MAGIC ON target.campaign_id = source.campaign_id AND target.lead_id = source.lead_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET target.mapping_date = source.mapping_date
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (campaign_id, lead_id, mapping_date)
# MAGIC   VALUES (source.campaign_id, source.lead_id, source.mapping_date);
