# Databricks notebook source
# DBTITLE 1,View: dim_contact_view
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_contact_view AS
# MAGIC SELECT DISTINCT
# MAGIC     id AS contact_id,
# MAGIC     first_name AS contact_name,
# MAGIC     email AS contact_email,
# MAGIC     mobile AS contact_phone,
# MAGIC     current_timestamp() AS created_date, -- Timestamp de la carga actual
# MAGIC     current_timestamp() AS modified_date -- Asumimos que toda la carga es reciente
# MAGIC FROM silver_lakehouse.zohocontacts;

# COMMAND ----------

# DBTITLE 1,dim_contact
# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.dim_contact AS target
# MAGIC USING dim_contact_view AS source
# MAGIC ON target.contact_id = source.contact_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET 
# MAGIC     target.contact_name = source.contact_name,
# MAGIC     target.contact_email = source.contact_email,
# MAGIC     target.contact_phone = source.contact_phone,
# MAGIC     target.modified_date = source.modified_date
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     contact_id, contact_name, contact_email, contact_phone, created_date, modified_date
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.contact_id, source.contact_name, source.contact_email, source.contact_phone, 
# MAGIC     source.created_date, source.modified_date
# MAGIC   );

# COMMAND ----------

# DBTITLE 1,View: dim_opportunity_view
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_opportunity_view AS
# MAGIC SELECT DISTINCT
# MAGIC     FIRST_VALUE(deals.id) OVER (PARTITION BY deals.id) AS opportunity_id,
# MAGIC     FIRST_VALUE(deals.deal_name) OVER (PARTITION BY deals.id) AS opportunity_name,
# MAGIC     FIRST_VALUE(deals.etapa) OVER (PARTITION BY deals.id) AS opportunity_stage,
# MAGIC     FIRST_VALUE(deals.probabilidad) OVER (PARTITION BY deals.id) AS opportunity_probability,
# MAGIC     FIRST_VALUE(deals.importe) OVER (PARTITION BY deals.id) AS opportunity_amount,
# MAGIC     current_timestamp() AS created_date,
# MAGIC     MAX(deals.modified_time) OVER (PARTITION BY deals.id) AS modified_date
# MAGIC FROM silver_lakehouse.zohodeals deals;

# COMMAND ----------

# DBTITLE 1,dim_opportunity
# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.dim_opportunity AS target
# MAGIC USING dim_opportunity_view AS source
# MAGIC ON target.opportunity_id = source.opportunity_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET 
# MAGIC     target.opportunity_name = source.opportunity_name,
# MAGIC     target.opportunity_stage = source.opportunity_stage,
# MAGIC     target.opportunity_probability = source.opportunity_probability,
# MAGIC     target.opportunity_amount = source.opportunity_amount,
# MAGIC     target.modified_date = source.modified_date
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     opportunity_id, opportunity_name, opportunity_stage, opportunity_probability, 
# MAGIC     opportunity_amount, created_date, modified_date
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.opportunity_id, source.opportunity_name, source.opportunity_stage, 
# MAGIC     source.opportunity_probability, source.opportunity_amount, source.created_date, 
# MAGIC     source.modified_date
# MAGIC   );

# COMMAND ----------

# DBTITLE 1,View: dim_lead_view
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_lead_view AS
# MAGIC SELECT DISTINCT
# MAGIC     leads.id AS lead_id,
# MAGIC     COALESCE(contacts.first_name, leads.first_name) AS lead_name, -- Prioridad: Contactos > Leads
# MAGIC     COALESCE(contacts.email, leads.email) AS lead_email,         -- Prioridad: Contactos > Leads
# MAGIC     leads.lead_status,
# MAGIC     leads.lead_rating,
# MAGIC     current_timestamp() AS created_date,
# MAGIC     leads.modified_time AS modified_date
# MAGIC FROM silver_lakehouse.zoholeads leads
# MAGIC LEFT JOIN silver_lakehouse.zohocontacts contacts
# MAGIC     ON leads.email = contacts.email; -- Relación entre Leads y Contactos.
# MAGIC

# COMMAND ----------

# DBTITLE 1,dim_lead
# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_lead AS target
# MAGIC USING dim_lead_view AS source
# MAGIC ON target.lead_id = source.lead_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET 
# MAGIC     target.lead_name = source.lead_name,
# MAGIC     target.lead_email = source.lead_email,
# MAGIC     target.lead_status = source.lead_status,
# MAGIC     target.lead_rating = source.lead_rating,
# MAGIC     target.modified_date = source.modified_date
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     lead_id, lead_name, lead_email, lead_status, lead_rating, created_date, modified_date
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.lead_id, source.lead_name, source.lead_email, source.lead_status, 
# MAGIC     source.lead_rating, source.created_date, source.modified_date
# MAGIC   );

# COMMAND ----------

# DBTITLE 1,View: dim_zoho_view
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_zoho_view AS
# MAGIC SELECT DISTINCT
# MAGIC     FIRST_VALUE(leads.id) OVER (PARTITION BY leads.id) AS lead_id,
# MAGIC     FIRST_VALUE(contacts.id) OVER (PARTITION BY leads.id) AS contact_id,
# MAGIC     FIRST_VALUE(deals.id) OVER (PARTITION BY leads.id) AS opportunity_id,
# MAGIC     FIRST_VALUE(COALESCE(deals.deal_name, leads.first_name)) OVER (PARTITION BY leads.id) AS opportunity_name,
# MAGIC     FIRST_VALUE(COALESCE(deals.etapa, leads.lead_status)) OVER (PARTITION BY leads.id) AS opportunity_stage,
# MAGIC     FIRST_VALUE(COALESCE(deals.probabilidad, 0)) OVER (PARTITION BY leads.id) AS opportunity_probability,
# MAGIC     FIRST_VALUE(COALESCE(deals.importe, 0.0)) OVER (PARTITION BY leads.id) AS opportunity_amount,
# MAGIC     FIRST_VALUE(COALESCE(deals.etapa, leads.lead_status)) OVER (PARTITION BY leads.id) AS prioritized_field_1,
# MAGIC     FIRST_VALUE(COALESCE(deals.deal_name, contacts.first_name)) OVER (PARTITION BY leads.id) AS prioritized_field_2,
# MAGIC     FIRST_VALUE(leads.first_name) OVER (PARTITION BY leads.id) AS lead_name,
# MAGIC     FIRST_VALUE(leads.email) OVER (PARTITION BY leads.id) AS lead_email,
# MAGIC     FIRST_VALUE(contacts.first_name) OVER (PARTITION BY leads.id) AS contact_name,
# MAGIC     FIRST_VALUE(contacts.email) OVER (PARTITION BY leads.id) AS contact_email,
# MAGIC     FIRST_VALUE(contacts.mobile) OVER (PARTITION BY leads.id) AS contact_phone,
# MAGIC     current_timestamp() AS created_date,
# MAGIC     MAX(GREATEST(
# MAGIC         deals.modified_time, 
# MAGIC         leads.modified_time
# MAGIC         -- Aquí omitimos contacts.modified_time ya que no existe
# MAGIC     )) OVER (PARTITION BY leads.id) AS modified_date
# MAGIC FROM silver_lakehouse.zoholeads leads
# MAGIC LEFT JOIN silver_lakehouse.zohocontacts contacts
# MAGIC     ON leads.email = contacts.email
# MAGIC LEFT JOIN silver_lakehouse.zohodeals deals
# MAGIC     ON leads.id_correlacion_prospecto = deals.id_prospecto;

# COMMAND ----------

# DBTITLE 1,dim_zoho
# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.dim_zoho AS target
# MAGIC USING dim_zoho_view AS source
# MAGIC ON target.lead_id = source.lead_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET 
# MAGIC     target.contact_id = source.contact_id,
# MAGIC     target.opportunity_id = source.opportunity_id,
# MAGIC     target.opportunity_name = source.opportunity_name,
# MAGIC     target.opportunity_stage = source.opportunity_stage,
# MAGIC     target.opportunity_probability = source.opportunity_probability,
# MAGIC     target.opportunity_amount = source.opportunity_amount,
# MAGIC     target.prioritized_field_1 = source.prioritized_field_1,
# MAGIC     target.prioritized_field_2 = source.prioritized_field_2,
# MAGIC     target.lead_name = source.lead_name,
# MAGIC     target.lead_email = source.lead_email,
# MAGIC     target.contact_name = source.contact_name,
# MAGIC     target.contact_email = source.contact_email,
# MAGIC     target.contact_phone = source.contact_phone,
# MAGIC     target.modified_date = source.modified_date
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     lead_id, contact_id, opportunity_id, opportunity_name, opportunity_stage, 
# MAGIC     opportunity_probability, opportunity_amount, prioritized_field_1, 
# MAGIC     prioritized_field_2, lead_name, lead_email, contact_name, contact_email, 
# MAGIC     contact_phone, created_date, modified_date
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.lead_id, source.contact_id, source.opportunity_id, source.opportunity_name, 
# MAGIC     source.opportunity_stage, source.opportunity_probability, source.opportunity_amount, 
# MAGIC     source.prioritized_field_1, source.prioritized_field_2, source.lead_name, 
# MAGIC     source.lead_email, source.contact_name, source.contact_email, source.contact_phone, 
# MAGIC     source.created_date, source.modified_date
# MAGIC   );
