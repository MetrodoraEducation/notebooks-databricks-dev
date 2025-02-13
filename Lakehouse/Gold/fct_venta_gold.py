# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW silver_venta_view
# MAGIC     AS SELECT * FROM silver_lakehouse.sales where fec_procesamiento > (select IFNULL(max(fec_procesamiento),'1900-01-01') from gold_lakehouse.fct_venta);
# MAGIC
# MAGIC select * from silver_venta_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW modalidad_mapeo_view AS
# MAGIC SELECT a.cod_venta, a.modalidad,
# MAGIC IFNULL(b.modalidad_norm, 'n/a') AS modalidad_norm
# MAGIC FROM silver_venta_view a
# MAGIC left JOIN  gold_lakehouse.mapeo_modalidad b
# MAGIC on  a.modalidad = b.modalidad;
# MAGIC
# MAGIC select * from modalidad_mapeo_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW modalidad_norm_view AS
# MAGIC SELECT a.cod_venta, a.modalidad,a.modalidad_norm, b.id_dim_modalidad
# MAGIC FROM modalidad_mapeo_view a
# MAGIC left JOIN  gold_lakehouse.dim_modalidad b
# MAGIC on  a.modalidad_norm = b.nombre_modalidad

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW sede_mapeo_view AS
# MAGIC SELECT a.cod_venta, a.sede,
# MAGIC IFNULL(b.sede_norm, 'n/a') AS sede_norm
# MAGIC FROM silver_venta_view a
# MAGIC left JOIN  gold_lakehouse.mapeo_sede b
# MAGIC on  a.sede = b.sede

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW sede_norm_view AS
# MAGIC SELECT a.cod_venta, a.sede,a.sede_norm, b.id_dim_sede
# MAGIC FROM sede_mapeo_view a
# MAGIC left JOIN  gold_lakehouse.dim_sede b
# MAGIC on  a.sede_norm = b.nombre_sede

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW estudio_mapeo_view AS
# MAGIC SELECT a.cod_venta, a.titulacion, a.sistema_origen,
# MAGIC case when b.estudio_norm is null and a.sistema_origen='Clientify' then 'CFGEN'
# MAGIC  when b.estudio_norm is null and a.sistema_origen='Odoo' then 'IPGEN'
# MAGIC  else b.estudio_norm end as estudio_norm
# MAGIC
# MAGIC FROM silver_venta_view a
# MAGIC left JOIN  gold_lakehouse.mapeo_estudio b
# MAGIC on  a.titulacion = b.estudio

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW estudio_norm_view AS
# MAGIC SELECT a.cod_venta, a.titulacion, a.sistema_origen, a.estudio_norm, b.id_dim_estudio, c.id_dim_tipo_negocio, d.id_dim_tipo_formacion
# MAGIC FROM estudio_mapeo_view a
# MAGIC left JOIN  gold_lakehouse.dim_estudio b on  a.estudio_norm = b.cod_estudio
# MAGIC left JOIN  gold_lakehouse.dim_tipo_negocio c on  b.tipo_negocio_desc = c.tipo_negocio_desc
# MAGIC left JOIN  gold_lakehouse.dim_tipo_formacion d on  b.tipo_formacion_desc = d.tipo_formacion_desc

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cruce con Ventas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fct_venta_view
# MAGIC     AS SELECT 
# MAGIC     a.cod_venta,
# MAGIC     a.nombre,
# MAGIC     a.email,
# MAGIC     a.telefono,
# MAGIC     a.nombre_contacto,
# MAGIC     b.id_dim_comercial as id_dim_propietario_lead,
# MAGIC     --id_dim_origen_campania,
# MAGIC     c.id_dim_origen_campania,
# MAGIC     d.id_dim_campania,
# MAGIC     a.importe_venta,
# MAGIC     a.importe_descuento,
# MAGIC     (a.importe_venta-a.importe_descuento-a.importe_descuento_matricula) as importe_venta_neta,
# MAGIC     e.id_dim_estado_venta,
# MAGIC     f.id_dim_etapa_venta,
# MAGIC     a.posibilidad_venta,
# MAGIC     to_date(a.fec_creacion) as fec_creacion,
# MAGIC     to_date(a.fec_modificacion) as fec_modificacion,
# MAGIC     to_date(a.fec_cierre) as fec_cierre,
# MAGIC     --id_dim_modalidad,
# MAGIC     g.id_dim_modalidad as id_dim_modalidad,
# MAGIC     h.id_dim_institucion,
# MAGIC     --id_dim_sede,
# MAGIC     i.id_dim_sede    as id_dim_sede,
# MAGIC     j.id as id_dim_pais,
# MAGIC     --id_dim_estudio,
# MAGIC     k.id_dim_estudio,
# MAGIC     to_date(a.fec_pago_matricula) as fec_pago_matricula,
# MAGIC     a.importe_matricula, 
# MAGIC     a.importe_descuento_matricula,
# MAGIC     (a.importe_matricula-a.importe_descuento_matricula) as importe_neto_matricula,
# MAGIC     l.id_dim_localidad,
# MAGIC     --id_dim_tipo_formacion,
# MAGIC     k.id_dim_tipo_formacion,
# MAGIC     --id_dim_tipo_negocio,
# MAGIC     k.id_dim_tipo_negocio,
# MAGIC     a.nombre_scoring,
# MAGIC     a.puntos_scoring,
# MAGIC     date_diff(a.fec_cierre, a.fec_creacion ) as dias_cierre,
# MAGIC     o.id_dim_motivo_cierre,
# MAGIC     a.fec_procesamiento,
# MAGIC     a.sistema_origen,
# MAGIC     case when (a.tiempo_de_maduracion is null or a.tiempo_de_maduracion ='') then 0 else a.tiempo_de_maduracion end as tiempo_de_maduracion,
# MAGIC     a.new_enrollent,
# MAGIC     a.lead_neto,
# MAGIC     a.activo
# MAGIC     
# MAGIC     FROM silver_venta_view a 
# MAGIC     LEFT JOIN gold_lakehouse.dim_comercial b ON a.propietario_lead = b.nombre_comercial
# MAGIC     LEFT JOIN gold_lakehouse.dim_origen_campania c ON a.origen_campania = c.nombre_origen_campania --mapeo
# MAGIC     LEFT JOIN gold_lakehouse.dim_campania d ON a.campania = d.nombre_campania
# MAGIC     LEFT JOIN gold_lakehouse.dim_estado_venta e ON a.estado_venta = e.nombre_estado_venta
# MAGIC     LEFT JOIN gold_lakehouse.dim_etapa_venta f ON a.etapa_venta = f.nombre_etapa_venta
# MAGIC     LEFT JOIN modalidad_norm_view g ON a.cod_venta = g.cod_venta --mapeo
# MAGIC     LEFT JOIN gold_lakehouse.dim_institucion h ON a.institucion = h.nombre_institucion
# MAGIC     LEFT JOIN sede_norm_view i ON a.cod_venta = i.cod_venta --mapeo
# MAGIC     LEFT JOIN gold_lakehouse.dim_pais j ON a.pais = j.name
# MAGIC     LEFT JOIN estudio_norm_view k ON a.cod_venta  = k.cod_venta --mapeo
# MAGIC     LEFT JOIN gold_lakehouse.dim_localidad l ON a.localidad = l.nombre_localidad
# MAGIC     --LEFT JOIN gold_lakehouse.dim_tipo_formacion m ON a.id_dim_tipo_formacion = m.id_dim_tipo_formacion --mapeo
# MAGIC     --LEFT JOIN gold_lakehouse.dim_tipo_negocio n ON a.id_dim_tipo_negocio = n.id_dim_tipo_negocio --mapeo
# MAGIC     LEFT JOIN gold_lakehouse.dim_motivo_cierre o ON a.motivo_cierre = o.motivo_cierre
# MAGIC

# COMMAND ----------

fct_venta_df = spark.sql("select * from fct_venta_view")

# COMMAND ----------

fct_venta_df.createOrReplaceTempView("fct_venta_view")

# COMMAND ----------

fct_venta_df = fct_venta_df.dropDuplicates()

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE WITH SCHEMA EVOLUTION 
# MAGIC INTO gold_lakehouse.fct_venta
# MAGIC USING fct_venta_view 
# MAGIC ON gold_lakehouse.fct_venta.cod_venta = fct_venta_view.cod_venta and gold_lakehouse.fct_venta.sistema_origen = fct_venta_view.sistema_origen
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cod_venta, COUNT(*)
# MAGIC FROM fct_venta_view
# MAGIC GROUP BY cod_venta
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cruce con tablas ZOHO

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW producto_con_institucion AS
# MAGIC SELECT 
# MAGIC     p.codProducto,  
# MAGIC     COALESCE(i.id_dim_institucion, 1) AS id_dim_institucion  -- Si no hay match, asigna un valor por defecto
# MAGIC FROM gold_lakehouse.dim_producto p
# MAGIC LEFT JOIN gold_lakehouse.dim_institucion i 
# MAGIC ON UPPER(TRIM(p.entidadLegal)) = UPPER(TRIM(i.nombre_institucion));  -- Normaliza el match
# MAGIC
# MAGIC select * from producto_con_institucion;

# COMMAND ----------

# DBTITLE 1,Cruce Zoho
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW zoho_table_view AS
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN leads.id  -- Con lead sin oportunidad
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN leads.id  -- Con lead y con oportunidad
# MAGIC         ELSE NULL  -- Oportunidad sin lead
# MAGIC     END AS cod_lead,
# MAGIC     CASE 
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN deals.id_lead  -- Con lead sin oportunidad
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN deals.id_lead  -- Con lead y con oportunidad
# MAGIC         WHEN leads.id IS NULL THEN deals.id_lead  -- Oportunidad sin lead
# MAGIC     END AS cod_oportunidad,
# MAGIC     CASE 
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN leads.Description  -- Con lead sin oportunidad
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN deals.Deal_name  -- Con lead y con oportunidad
# MAGIC         WHEN leads.id IS NULL THEN deals.Deal_name  -- Oportunidad sin lead
# MAGIC     END AS nombre,
# MAGIC     CASE 
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN leads.email  -- Con lead sin oportunidad
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN contacts.email  -- Con lead y con oportunidad
# MAGIC         WHEN leads.id IS NULL THEN contacts.email  -- Oportunidad sin lead
# MAGIC     END AS email,
# MAGIC     CASE 
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN leads.mobile  -- Con lead sin oportunidad
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN contacts.phone  -- Con lead y con oportunidad
# MAGIC         WHEN leads.id IS NULL THEN contacts.phone  -- Oportunidad sin lead
# MAGIC     END AS telefono,
# MAGIC     CASE 
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN CONCAT(leads.first_name, ' ', leads.last_name)  -- Con lead sin oportunidad
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN CONCAT(contacts.first_name, ' ', contacts.last_name)  -- Con lead y con oportunidad
# MAGIC         WHEN leads.id IS NULL THEN CONCAT(contacts.first_name, ' ', contacts.last_name)  -- Oportunidad sin lead
# MAGIC     END AS nombre_contacto,
# MAGIC     CASE 
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN 0  -- Con lead sin oportunidad
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN deals.importe  -- Con lead y con oportunidad
# MAGIC         WHEN leads.id IS NULL THEN deals.importe  -- Oportunidad sin lead
# MAGIC     END AS importe_venta,
# MAGIC     CASE 
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN 0   -- Con lead sin oportunidad
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN deals.descuento  -- Con lead y con oportunidad
# MAGIC         WHEN leads.id IS NULL THEN deals.descuento  -- Oportunidad sin lead
# MAGIC     END AS importe_descuento,
# MAGIC     CASE 
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN 0   -- Con lead sin oportunidad
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN (deals.importe - deals.descuento)  -- Con lead y con oportunidad
# MAGIC         WHEN leads.id IS NULL THEN (deals.importe - deals.descuento)  -- Oportunidad sin lead
# MAGIC     END AS importe_venta_neta,
# MAGIC     CASE 
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN NULL  
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN deals.probabilidad  
# MAGIC         WHEN leads.id IS NULL THEN deals.probabilidad  
# MAGIC     END AS posibilidad_venta,
# MAGIC     CASE 
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN leads.modified_time  
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN deals.modified_time  
# MAGIC         WHEN leads.id IS NULL THEN deals.modified_time  
# MAGIC     END AS fec_creacion,
# MAGIC     CASE 
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN leads.modified_time  
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN deals.modified_time  
# MAGIC         WHEN leads.id IS NULL THEN deals.modified_time  
# MAGIC     END AS fec_modificacion,
# MAGIC     CASE 
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN NULL  
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN deals.fecha_cierre  
# MAGIC         WHEN leads.id IS NULL THEN deals.fecha_cierre  
# MAGIC     END AS fec_cierre,
# MAGIC     CASE 
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN NULL  
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN deals.fecha_hora_pagado  
# MAGIC         WHEN leads.id IS NULL THEN deals.fecha_hora_pagado  
# MAGIC     END AS fec_pago_matricula,
# MAGIC     CASE 
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN leads.lead_rating  
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN leads.lead_rating  
# MAGIC         WHEN leads.id IS NULL THEN NULL  
# MAGIC     END AS nombre_scoring,
# MAGIC     CASE 
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN leads.lead_scoring  
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN leads.lead_scoring  
# MAGIC         WHEN leads.id IS NULL THEN NULL  
# MAGIC     END AS puntos_scoring,
# MAGIC     CASE 
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN DATE_DIFF(deals.fecha_cierre, leads.modified_time)    
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN DATE_DIFF(deals.fecha_cierre, leads.modified_time)  
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN DATE_DIFF(deals.fecha_cierre, leads.modified_time)    
# MAGIC     END AS dias_cierre,
# MAGIC     CASE 
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN leads.motivos_perdida  -- Con lead sin oportunidad
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN COALESCE(deals.motivo_perdida_b2b, deals.motivo_perdida_b2c)  -- Con lead y con oportunidad
# MAGIC         WHEN leads.id IS NULL AND deals.id_lead IS NOT NULL THEN COALESCE(deals.motivo_perdida_b2b, deals.motivo_perdida_b2c)  -- Oportunidad sin lead
# MAGIC     END AS motivo_perdida_mapeado,
# MAGIC     CASE 
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN leads.lead_status  -- Con lead sin oportunidad
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN deals.etapa  -- Con lead y con oportunidad
# MAGIC         WHEN leads.id IS NULL AND deals.id_lead IS NOT NULL THEN deals.etapa  -- Oportunidad sin lead
# MAGIC     END AS etapa_venta_mapeada,  -- Mapeo de la etapa de venta
# MAGIC         CASE 
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN leads.id  -- Con lead sin oportunidad
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN deals.id  -- Con lead y con oportunidad
# MAGIC         WHEN leads.id IS NULL AND deals.id_lead IS NOT NULL THEN deals.id  -- Oportunidad sin lead
# MAGIC     END AS propietario_lead_mapeado,
# MAGIC     CASE 
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN 0  -- Lead sin oportunidad
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN deals.descuento  -- Lead con oportunidad
# MAGIC         WHEN leads.id IS NULL AND deals.id_lead IS NOT NULL THEN deals.descuento  -- Oportunidad sin lead
# MAGIC     END AS importeDescuentoMatricula,
# MAGIC     CASE 
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN 0  -- Lead sin oportunidad
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN deals.importe  -- Lead con oportunidad
# MAGIC         WHEN leads.id IS NULL AND deals.id_lead IS NOT NULL THEN deals.importe  -- Oportunidad sin lead
# MAGIC     END AS importeNetoMatricula,
# MAGIC         CASE 
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN importeDescuentoMatricula + importeNetoMatricula  -- Lead sin oportunidad
# MAGIC         WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN importeDescuentoMatricula + importeNetoMatricula  -- Lead con oportunidad
# MAGIC         WHEN leads.id IS NULL AND deals.id_lead IS NOT NULL THEN importeDescuentoMatricula + importeNetoMatricula  -- Oportunidad sin lead
# MAGIC     END AS importeMatricula,
# MAGIC     deals.id_producto
# MAGIC FROM silver_lakehouse.zoholeads leads
# MAGIC FULL OUTER JOIN silver_lakehouse.zohodeals deals
# MAGIC ON leads.id = deals.id_lead
# MAGIC FULL OUTER JOIN silver_lakehouse.zohocontacts contacts
# MAGIC ON deals.id = contacts.id;
# MAGIC
# MAGIC select * from zoho_table_view

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW zoho_table_view_temp AS
# MAGIC SELECT 
# MAGIC     zt.cod_lead
# MAGIC     ,zt.cod_oportunidad
# MAGIC     ,zt.nombre
# MAGIC     ,zt.email
# MAGIC     ,zt.telefono
# MAGIC     ,zt.nombre_contacto
# MAGIC     ,dc.id_dim_comercial AS id_dim_propietario_lead
# MAGIC     ,zt.importe_venta
# MAGIC     ,zt.importe_descuento
# MAGIC     ,zt.importe_venta_neta as importe_venta_neta
# MAGIC     ,null as id_dim_estado_venta --PENDIENTE CONFIRMAR ZOHO
# MAGIC     ,ev.id_dim_etapa_venta AS id_dim_etapa_venta
# MAGIC     ,zt.posibilidad_venta
# MAGIC     ,zt.fec_creacion as fec_creacion
# MAGIC     ,zt.fec_modificacion as fec_modificacion
# MAGIC     ,zt.fec_cierre as fec_cierre
# MAGIC     ,pr.iddimprograma as id_dim_programa
# MAGIC     ,m.id_dim_modalidad as id_dim_modalidad
# MAGIC     ,pci.id_dim_institucion as id_dim_institucion
# MAGIC     ,s.id_dim_sede as id_dim_sede
# MAGIC     ,p.iddimproducto as id_dim_producto
# MAGIC     ,tf.id_dim_tipo_formacion AS id_dim_tipo_formacion  
# MAGIC     ,tn.id_dim_tipo_negocio AS id_dim_tipo_negocio
# MAGIC     ,dp.id AS id_dim_pais
# MAGIC     ,zt.fec_pago_matricula as fec_pago_matricula
# MAGIC     ,zt.importeMatricula as importe_Matricula
# MAGIC     ,zt.importeDescuentoMatricula as importe_Descuento_Matricula
# MAGIC     ,zt.importeNetoMatricula as importe_Neto_Matricula
# MAGIC     ,zt.nombre_scoring as nombre_scoring
# MAGIC     ,zt.puntos_scoring as puntos_scoring
# MAGIC     ,zt.dias_cierre as dias_cierre
# MAGIC     ,mp.iddimmotivoperdida AS id_dim_motivo_perdida  
# MAGIC     ,utm_campaign.id_dim_utm_campaign AS id_dim_utm_campaign
# MAGIC     ,utm_adset.id_dim_utm_ad AS id_dim_utm_ad
# MAGIC     ,utm_source.id_dim_utm_source AS id_dim_utm_source
# MAGIC FROM (
# MAGIC     SELECT 
# MAGIC         zoho.*
# MAGIC         ,CASE 
# MAGIC             WHEN zoho.cod_lead IS NOT NULL AND zoho.cod_oportunidad IS NULL THEN zoho.id_producto  -- Con lead sin oportunidad
# MAGIC             WHEN zoho.cod_lead IS NOT NULL AND zoho.cod_oportunidad IS NOT NULL THEN zoho.id_producto  -- Con lead y con oportunidad
# MAGIC             WHEN zoho.cod_lead IS NULL AND zoho.cod_oportunidad IS NOT NULL THEN zoho.id_producto  -- Oportunidad sin lead
# MAGIC         END AS id_producto_mapeado
# MAGIC         ,CASE 
# MAGIC             WHEN zoho.cod_lead IS NOT NULL AND zoho.cod_oportunidad IS NULL THEN leads.Residencia  
# MAGIC             WHEN zoho.cod_lead IS NOT NULL AND zoho.cod_oportunidad IS NOT NULL THEN deals.Residencia1 
# MAGIC             WHEN zoho.cod_lead IS NULL AND zoho.cod_oportunidad IS NOT NULL THEN deals.Residencia1  
# MAGIC         END AS residencia_mapeada
# MAGIC         ,CASE 
# MAGIC             WHEN zoho.cod_lead IS NOT NULL AND zoho.cod_oportunidad IS NULL THEN leads.utm_campaign_id  
# MAGIC             WHEN zoho.cod_lead IS NOT NULL AND zoho.cod_oportunidad IS NOT NULL THEN deals.utm_campaign_id  
# MAGIC             WHEN zoho.cod_lead IS NULL AND zoho.cod_oportunidad IS NOT NULL THEN deals.utm_campaign_id  
# MAGIC         END AS utm_campaign_id_mapeada
# MAGIC         ,CASE 
# MAGIC             WHEN zoho.cod_lead IS NOT NULL AND zoho.cod_oportunidad IS NULL THEN leads.utm_ad_id  
# MAGIC             WHEN zoho.cod_lead IS NOT NULL AND zoho.cod_oportunidad IS NOT NULL THEN deals.utm_ad_id  
# MAGIC             WHEN zoho.cod_lead IS NULL AND zoho.cod_oportunidad IS NOT NULL THEN deals.utm_ad_id  
# MAGIC         END AS utm_ad_id_mapeada
# MAGIC         ,CASE 
# MAGIC             WHEN zoho.cod_lead IS NOT NULL AND zoho.cod_oportunidad IS NULL THEN leads.utm_source  
# MAGIC             WHEN zoho.cod_lead IS NOT NULL AND zoho.cod_oportunidad IS NOT NULL THEN deals.utm_source  
# MAGIC             WHEN zoho.cod_lead IS NULL AND zoho.cod_oportunidad IS NOT NULL THEN deals.utm_source  
# MAGIC         END AS utm_source_mapeada
# MAGIC         --,COALESCE(deals.processdate, leads.processdate, current_timestamp()) AS processdate
# MAGIC     FROM zoho_table_view zoho
# MAGIC     LEFT JOIN silver_lakehouse.zoholeads leads ON zoho.cod_lead = leads.id  -- Cruce con leads para obtener Residencia
# MAGIC     LEFT JOIN silver_lakehouse.zohodeals deals ON zoho.cod_oportunidad = deals.id_lead  -- Cruce con deals para obtener Residencia1
# MAGIC ) zt
# MAGIC LEFT JOIN gold_lakehouse.dim_producto p ON p.codProducto = zt.id_producto_mapeado 
# MAGIC LEFT JOIN gold_lakehouse.dim_programa pr ON p.codprograma = pr.codprograma  -- Mapeo del id_dim_programa
# MAGIC LEFT JOIN gold_lakehouse.dim_modalidad m ON p.modalidad = m.nombre_modalidad  -- Mapeo del id_dim_modalidad
# MAGIC LEFT JOIN gold_lakehouse.dim_sede s ON SUBSTRING(p.codProducto, 20, 3) = s.codigo_sede  -- Mapeo del id_dim_sede
# MAGIC LEFT JOIN gold_lakehouse.dim_tipo_formacion tf ON p.tipoProducto = tf.tipo_formacion_desc  -- Relación entre tipoProducto y tipo_formacion_desc
# MAGIC LEFT JOIN gold_lakehouse.dim_tipo_negocio tn ON p.tipoNegocio = tn.tipo_negocio_desc
# MAGIC LEFT JOIN gold_lakehouse.dim_motivo_perdida mp ON zt.motivo_perdida_mapeado = mp.nombreDimMotivoPerdida
# MAGIC LEFT JOIN gold_lakehouse.dim_etapa_venta ev ON zt.etapa_venta_mapeada = ev.nombre_etapa_venta
# MAGIC LEFT JOIN gold_lakehouse.dim_comercial dc ON CAST(zt.propietario_lead_mapeado AS STRING) = CAST(dc.cod_comercial AS STRING)
# MAGIC LEFT JOIN producto_con_institucion pci ON pci.codProducto = zt.id_producto_mapeado
# MAGIC LEFT JOIN gold_lakehouse.dim_pais dp ON UPPER(TRIM(zt.residencia_mapeada)) = UPPER(TRIM(dp.nombre))
# MAGIC LEFT JOIN gold_lakehouse.dim_utm_campaign utm_campaign ON zt.utm_campaign_id_mapeada = utm_campaign.utm_campaign_id
# MAGIC LEFT JOIN gold_lakehouse.dim_utm_adset utm_adset ON zt.utm_ad_id_mapeada = utm_adset.utm_ad_id
# MAGIC LEFT JOIN gold_lakehouse.dim_utm_source utm_source ON zt.utm_source_mapeada = utm_source.utm_source;
# MAGIC
# MAGIC SELECT * FROM zoho_table_view_temp;
