# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tablon_leads_and_deals AS
# MAGIC                 SELECT  
# MAGIC                         CASE 
# MAGIC                                 WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN 1
# MAGIC                                 WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN 2
# MAGIC                                 WHEN leads.id IS NULL AND deals.id_lead IS NOT NULL THEN 3
# MAGIC                                 ELSE -1
# MAGIC                         END AS id_tipo_registro,
# MAGIC                         CASE 
# MAGIC                                 WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN 'Con lead sin oportunidad'
# MAGIC                                 WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN 'Con lead y con oportunidad'
# MAGIC                                 WHEN leads.id IS NULL AND deals.id_lead IS NOT NULL THEN 'Oportunidad sin lead'
# MAGIC                                 ELSE 'n/a'
# MAGIC                         END AS tipo_registro,
# MAGIC                         leads.id AS cod_Lead,
# MAGIC                         leads.Description AS lead_Nombre,
# MAGIC                         leads.First_Name AS Nombre,
# MAGIC                         leads.Last_Name AS Apellido1,
# MAGIC                         leads.Apellido_2 AS Apellido2,
# MAGIC                         leads.Email AS email,
# MAGIC                         leads.Mobile AS telefono1,
# MAGIC                         COALESCE(leads.Nacionalidad, deals.Nacionalidad1) AS nacionalidad,
# MAGIC                         leads.Phone AS telefono2,
# MAGIC                         leads.Provincia AS provincia,
# MAGIC                         COALESCE(leads.Residencia, deals.Residencia1) AS residencia,
# MAGIC                         leads.Sexo AS sexo,
# MAGIC                         COALESCE(leads.lead_rating, deals.br_rating) AS lead_Rating,
# MAGIC                         COALESCE(try_cast(leads.lead_scoring AS DOUBLE), try_cast(deals.br_score AS DOUBLE)) AS leadScoring,
# MAGIC                         COALESCE(leads.Lead_Status, deals.etapa) AS etapa,
# MAGIC                         COALESCE(leads.Motivos_perdida, deals.Motivo_perdida_B2B, deals.Motivo_perdida_B2C) AS motivo_Perdida,
# MAGIC                         deals.Probabilidad AS probabilidad_Conversion,
# MAGIC                         deals.Pipeline AS flujo_Venta,
# MAGIC                         deals.Profesion_Estudiante AS profesion_Estudiante,
# MAGIC                         deals.Competencia AS competencia,
# MAGIC                         leads.Tipologia_cliente AS tipo_Cliente_lead,
# MAGIC                         leads.tipo_conversion as tipo_conversion_lead,
# MAGIC                         COALESCE(leads.utm_ad_id, deals.utm_ad_id) AS utm_ad_id,
# MAGIC                         COALESCE(leads.utm_adset_id, deals.utm_adset_id) AS utm_adset_id,
# MAGIC                         COALESCE(leads.utm_campaign_id, deals.utm_campaign_id) AS utm_campaign_id,
# MAGIC                         COALESCE(leads.utm_campaign_name, deals.utm_campaign_name) AS utm_campaign_name,
# MAGIC                         COALESCE(leads.utm_channel, deals.utm_channel) AS utm_channel,
# MAGIC                         COALESCE(leads.utm_strategy, deals.utm_strategy) AS utm_estrategia,
# MAGIC                         COALESCE(leads.utm_medium, deals.utm_medium) AS utm_medium,
# MAGIC                         COALESCE(leads.utm_profile, deals.utm_profile) AS utm_perfil,
# MAGIC                         COALESCE(leads.utm_source, deals.utm_source) AS utm_source,
# MAGIC                         COALESCE(leads.utm_term, deals.utm_term) AS utm_term,
# MAGIC                         COALESCE(leads.utm_type, deals.utm_type) AS utm_type,
# MAGIC                         COALESCE(leads.Owner_id, deals.Owner_id) AS cod_Owner,
# MAGIC                         COALESCE(leads.id_producto, deals.ID_Producto) AS cod_Producto,
# MAGIC                         COALESCE(leads.lead_correlation_id, deals.lead_correlation_id) AS lead_Correlation,
# MAGIC                         leads.Created_Time AS fecha_Creacion_Lead, --leads.Created_Time
# MAGIC                         leads.Modified_Time AS fecha_Modificacion_Lead,
# MAGIC                         deals.id AS cod_Oportunidad,
# MAGIC                         deals.ID_Classlife AS cod_Classlife,
# MAGIC                         deals.Deal_Name AS nombre_Oportunidad,
# MAGIC                         deals.contact_name_id AS cod_Contacto,
# MAGIC                         deals.fecha_Cierre AS fecha_Cierre,
# MAGIC                         deals.id_unico AS cod_Unico_Zoho,
# MAGIC                         deals.Exchange_Rate AS ratio_Moneda,
# MAGIC                         deals.Currency AS moneda,
# MAGIC                         deals.Importe_pagado AS importe_Pagado,
# MAGIC                         deals.Codigo_descuento AS cod_Descuento,
# MAGIC                         deals.Descuento AS pct_Descuento,
# MAGIC                         deals.importe AS importe,
# MAGIC                         deals.Tipologia_alumno1 AS tipo_Alumno,
# MAGIC                         deals.tipo_conversion AS tipo_Conversion_opotunidad,
# MAGIC                         deals.Tipologia_cliente AS tipo_Cliente_oportunidad,
# MAGIC                         deals.fecha_hora_Pagado as fecha_hora_Pagado,
# MAGIC                         deals.Created_Time AS fecha_Creacion_Oportunidad, --deals.Created_Time
# MAGIC                         deals.Modified_Time AS fecha_Modificacion_Oportunidad,
# MAGIC                         CASE 
# MAGIC                             WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN leads.processdate
# MAGIC                             WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN COALESCE(deals.processdate, leads.processdate)
# MAGIC                             WHEN leads.id IS NULL AND deals.id_lead IS NOT NULL THEN deals.processdate
# MAGIC                         ELSE NULL
# MAGIC                         END AS processdate,
# MAGIC                         CASE 
# MAGIC                             WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN leads.sourcesystem
# MAGIC                             WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN COALESCE(deals.sourcesystem, leads.sourcesystem)
# MAGIC                             WHEN leads.id IS NULL AND deals.id_lead IS NOT NULL THEN deals.sourcesystem
# MAGIC                         ELSE NULL
# MAGIC                         END AS sourcesystem
# MAGIC                         --,ROW_NUMBER() OVER (PARTITION BY deals.id_lead ORDER BY deals.Modified_Time DESC) AS row_num_version
# MAGIC                 FROM silver_lakehouse.zoholeads leads
# MAGIC      FULL OUTER JOIN silver_lakehouse.zohodeals deals
# MAGIC                   ON leads.id = deals.id_lead;
# MAGIC
# MAGIC SELECT * FROM tablon_leads_and_deals;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.tablon_leads_and_deals AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT * FROM tablon_leads_and_deals
# MAGIC ) AS source
# MAGIC ON target.cod_Oportunidad = source.cod_Oportunidad 
# MAGIC    OR (target.cod_Oportunidad IS NULL AND target.cod_Lead = source.cod_Lead)
# MAGIC
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.id_tipo_registro = source.id_tipo_registro,
# MAGIC         target.tipo_registro = source.tipo_registro,
# MAGIC         target.cod_Lead = source.cod_Lead,
# MAGIC         target.lead_Nombre = source.lead_Nombre,
# MAGIC         target.Nombre = source.Nombre,
# MAGIC         target.Apellido1 = source.Apellido1,
# MAGIC         target.Apellido2 = source.Apellido2,
# MAGIC         target.email = source.email,
# MAGIC         target.telefono1 = source.telefono1,
# MAGIC         target.nacionalidad = source.nacionalidad,
# MAGIC         target.telefono2 = source.telefono2,
# MAGIC         target.provincia = source.provincia,
# MAGIC         target.residencia = source.residencia,
# MAGIC         target.sexo = source.sexo,
# MAGIC         target.lead_Rating = source.lead_Rating,
# MAGIC         target.leadScoring = source.leadScoring,
# MAGIC         target.etapa = source.etapa,
# MAGIC         target.motivo_Perdida = source.motivo_Perdida,
# MAGIC         target.probabilidad_Conversion = source.probabilidad_Conversion,
# MAGIC         target.flujo_Venta = source.flujo_Venta,
# MAGIC         target.profesion_Estudiante = source.profesion_Estudiante,
# MAGIC         target.competencia = source.competencia,
# MAGIC         target.tipo_Cliente_lead = source.tipo_Cliente_lead,
# MAGIC         target.tipo_conversion_lead = source.tipo_conversion_lead,
# MAGIC         target.utm_ad_id = source.utm_ad_id,
# MAGIC         target.utm_adset_id = source.utm_adset_id,
# MAGIC         target.utm_campaign_id = source.utm_campaign_id,
# MAGIC         target.utm_campaign_name = source.utm_campaign_name,
# MAGIC         target.utm_channel = source.utm_channel,
# MAGIC         target.utm_estrategia = source.utm_estrategia,
# MAGIC         target.utm_medium = source.utm_medium,
# MAGIC         target.utm_perfil = source.utm_perfil,
# MAGIC         target.utm_source = source.utm_source,
# MAGIC         target.utm_term = source.utm_term,
# MAGIC         target.utm_type = source.utm_type,
# MAGIC         target.cod_Owner = source.cod_Owner,
# MAGIC         target.cod_Producto = source.cod_Producto,
# MAGIC         target.lead_Correlation = source.lead_Correlation,
# MAGIC         target.fecha_Creacion_Lead = source.fecha_Creacion_Lead,
# MAGIC         target.fecha_Modificacion_Lead = source.fecha_Modificacion_Lead,
# MAGIC         target.cod_Oportunidad = source.cod_Oportunidad,
# MAGIC         target.cod_Classlife = source.cod_Classlife,
# MAGIC         target.nombre_Oportunidad = source.nombre_Oportunidad,
# MAGIC         target.cod_Contacto = source.cod_Contacto,
# MAGIC         target.fecha_Cierre = source.fecha_Cierre,
# MAGIC         target.cod_Unico_Zoho = source.cod_Unico_Zoho,
# MAGIC         target.ratio_Moneda = source.ratio_Moneda,
# MAGIC         target.moneda = source.moneda,
# MAGIC         target.importe_Pagado = source.importe_Pagado,
# MAGIC         target.cod_Descuento = source.cod_Descuento,
# MAGIC         target.pct_Descuento = source.pct_Descuento,
# MAGIC         target.importe = source.importe,
# MAGIC         target.tipo_Alumno = source.tipo_Alumno,
# MAGIC         target.tipo_Conversion_opotunidad = source.tipo_Conversion_opotunidad,
# MAGIC         target.tipo_Cliente_oportunidad = source.tipo_Cliente_oportunidad,
# MAGIC         target.fecha_hora_Pagado = source.fecha_hora_Pagado,
# MAGIC         target.fecha_Creacion_Oportunidad = source.fecha_Creacion_Oportunidad,
# MAGIC         target.fecha_Modificacion_Oportunidad = source.fecha_Modificacion_Oportunidad,
# MAGIC         target.processdate = source.processdate,
# MAGIC         target.sourcesystem = source.sourcesystem
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (
# MAGIC         id_tipo_registro, tipo_registro, cod_Lead, lead_Nombre, Nombre, Apellido1, Apellido2, 
# MAGIC         email, telefono1, nacionalidad, telefono2, provincia, residencia, sexo, lead_Rating, 
# MAGIC         leadScoring, etapa, motivo_Perdida, probabilidad_Conversion, flujo_Venta, 
# MAGIC         profesion_Estudiante, competencia, tipo_Cliente_lead, tipo_conversion_lead, 
# MAGIC         utm_ad_id, utm_adset_id, utm_campaign_id, utm_campaign_name, utm_channel, utm_estrategia, 
# MAGIC         utm_medium, utm_perfil, utm_source, utm_term, utm_type, cod_Owner, cod_Producto, 
# MAGIC         lead_Correlation, fecha_Creacion_Lead, fecha_Modificacion_Lead, cod_Oportunidad, cod_Classlife, 
# MAGIC         nombre_Oportunidad, cod_Contacto, fecha_Cierre, cod_Unico_Zoho, ratio_Moneda, moneda, 
# MAGIC         importe_Pagado, cod_Descuento, pct_Descuento, importe, tipo_Alumno, 
# MAGIC         tipo_Conversion_opotunidad, tipo_Cliente_oportunidad, fecha_hora_Pagado, fecha_Creacion_Oportunidad, fecha_Modificacion_Oportunidad, 
# MAGIC         processdate, sourcesystem
# MAGIC     ) 
# MAGIC     VALUES (
# MAGIC         source.id_tipo_registro, source.tipo_registro, source.cod_Lead, source.lead_Nombre, 
# MAGIC         source.Nombre, source.Apellido1, source.Apellido2, source.email, source.telefono1, 
# MAGIC         source.nacionalidad, source.telefono2, source.provincia, source.residencia, source.sexo, 
# MAGIC         source.lead_Rating, source.leadScoring, source.etapa, source.motivo_Perdida, 
# MAGIC         source.probabilidad_Conversion, source.flujo_Venta, source.profesion_Estudiante, 
# MAGIC         source.competencia, source.tipo_Cliente_lead, source.tipo_conversion_lead, 
# MAGIC         source.utm_ad_id, source.utm_adset_id, source.utm_campaign_id, source.utm_campaign_name, 
# MAGIC         source.utm_channel, source.utm_estrategia, source.utm_medium, source.utm_perfil, 
# MAGIC         source.utm_source, source.utm_term, source.utm_type, source.cod_Owner, source.cod_Producto, 
# MAGIC         source.lead_Correlation, source.fecha_Modificacion_Lead, source.cod_Oportunidad, source.fecha_Creacion_Lead, 
# MAGIC         source.cod_Classlife, source.nombre_Oportunidad, source.cod_Contacto, source.fecha_Cierre, 
# MAGIC         source.cod_Unico_Zoho, source.ratio_Moneda, source.moneda, source.importe_Pagado, 
# MAGIC         source.cod_Descuento, source.pct_Descuento, source.importe, source.tipo_Alumno, 
# MAGIC         source.tipo_Conversion_opotunidad, source.tipo_Cliente_oportunidad, source.fecha_hora_Pagado, source.fecha_Creacion_Oportunidad,
# MAGIC         source.fecha_Modificacion_Oportunidad, source.processdate, source.sourcesystem
# MAGIC     );

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from silver_lakehouse.tablon_leads_and_deals;
