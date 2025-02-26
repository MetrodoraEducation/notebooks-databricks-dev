# Databricks notebook source
# MAGIC %md
# MAGIC ### Cruce con tablas ZOHO

# COMMAND ----------

# DBTITLE 1,Cruce Zoho
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW zoho_table_view AS
# MAGIC    SELECT 
# MAGIC           tablon.id_tipo_registro
# MAGIC          ,tablon.tipo_registro
# MAGIC          ,CASE 
# MAGIC              WHEN tablon.id_tipo_registro = 1 THEN cod_Lead
# MAGIC              WHEN tablon.id_tipo_registro = 2 THEN cod_Lead
# MAGIC              WHEN tablon.id_tipo_registro = 3 THEN NULL
# MAGIC              ELSE NULL
# MAGIC          END AS cod_Lead
# MAGIC          ,tablon.cod_Oportunidad
# MAGIC          ,COALESCE(Nombre, nombre_Oportunidad) AS nombre
# MAGIC          ,COALESCE(
# MAGIC              CASE 
# MAGIC                  WHEN tablon.id_tipo_registro = 1 THEN tablon.email  -- Email de LEAD
# MAGIC              END, 
# MAGIC              contacts.email  -- Email de CONTACT
# MAGIC          ) AS email
# MAGIC          ,COALESCE(
# MAGIC              CASE 
# MAGIC                  WHEN tablon.id_tipo_registro = 1 THEN tablon.telefono1  -- telefono1 de LEAD
# MAGIC              END, 
# MAGIC              contacts.phone  -- phone de CONTACT
# MAGIC          ) AS telefono
# MAGIC          ,COALESCE(
# MAGIC              CASE 
# MAGIC                  WHEN tablon.id_tipo_registro = 1 THEN CONCAT(tablon.Nombre, ' ', tablon.Apellido1, ' ', tablon.Apellido2)  -- LEAD
# MAGIC              END, 
# MAGIC              CONCAT(contacts.First_Name, ' ', contacts.Last_Name, ' ', tablon.Apellido2)  -- CONTACT
# MAGIC          ) AS nombre_Contacto
# MAGIC          ,CASE 
# MAGIC              WHEN tablon.id_tipo_registro = 1 THEN 0
# MAGIC              WHEN tablon.id_tipo_registro IN (2,3) THEN COALESCE(ABS(tablon.pct_Descuento), 0)
# MAGIC              ELSE NULL
# MAGIC          END AS importe_Descuento
# MAGIC          ,CASE 
# MAGIC              WHEN tablon.id_tipo_registro = 1 THEN 0
# MAGIC              WHEN tablon.id_tipo_registro IN (2,3) THEN COALESCE(ABS(tablon.importe), 0)
# MAGIC              ELSE NULL
# MAGIC          END AS Importe_Venta_Neto
# MAGIC          ,CASE 
# MAGIC              WHEN tablon.id_tipo_registro = 1 THEN 0
# MAGIC              WHEN tablon.id_tipo_registro IN (2,3) THEN COALESCE(ABS(tablon.importe) + ABS(tablon.pct_Descuento), 0)
# MAGIC              ELSE NULL
# MAGIC          END AS Importe_Venta
# MAGIC          ,CASE 
# MAGIC              WHEN tablon.id_tipo_registro = 1 THEN 0
# MAGIC              WHEN tablon.id_tipo_registro IN (2,3) THEN COALESCE(tablon.probabilidad_Conversion / 100, 0)
# MAGIC              ELSE 0
# MAGIC          END AS posibilidad_Venta
# MAGIC          ,CASE 
# MAGIC              WHEN tablon.id_tipo_registro IN (1,2) THEN tablon.fecha_Creacion_Lead
# MAGIC              WHEN tablon.id_tipo_registro = 3 THEN tablon.fecha_Creacion_Oportunidad
# MAGIC              ELSE NULL
# MAGIC          END AS fec_Creacion
# MAGIC          ,CASE 
# MAGIC              WHEN tablon.id_tipo_registro = 1 THEN tablon.fecha_Modificacion_Lead
# MAGIC              WHEN tablon.id_tipo_registro IN (2,3) THEN tablon.fecha_Modificacion_Oportunidad
# MAGIC              ELSE NULL
# MAGIC          END AS fec_Modificacion
# MAGIC          ,CASE 
# MAGIC              WHEN tablon.id_tipo_registro = 1 THEN NULL
# MAGIC              WHEN tablon.id_tipo_registro IN (2,3) THEN tablon.fecha_Cierre
# MAGIC              ELSE NULL
# MAGIC          END AS fec_Cierre
# MAGIC          ,   CASE 
# MAGIC              WHEN tablon.id_tipo_registro = 1 THEN NULL
# MAGIC              WHEN tablon.id_tipo_registro IN (2,3) THEN tablon.fecha_hora_Pagado
# MAGIC              ELSE NULL
# MAGIC          END AS fec_Pago_Matricula
# MAGIC          ,CASE 
# MAGIC              WHEN tablon.id_tipo_registro IN (1,2) THEN tablon.lead_Rating
# MAGIC              WHEN tablon.id_tipo_registro = 3 THEN NULL
# MAGIC          END AS nombre_Scoring
# MAGIC          ,CASE 
# MAGIC              WHEN tablon.id_tipo_registro IN (1,2) THEN tablon.leadScoring
# MAGIC              WHEN tablon.id_tipo_registro = 3 THEN NULL
# MAGIC          END AS puntos_Scoring
# MAGIC          ,CASE 
# MAGIC              WHEN tablon.id_tipo_registro IN (1,2) THEN DATEDIFF(tablon.fecha_Cierre, tablon.fecha_Creacion_Lead)
# MAGIC              WHEN tablon.id_tipo_registro = 3 THEN DATEDIFF(tablon.fecha_Cierre, tablon.fecha_Creacion_Oportunidad)
# MAGIC          END AS dias_Cierre
# MAGIC          ,contacts.mailing_city AS ciudad
# MAGIC          ,contacts.provincia AS provincia
# MAGIC          ,contacts.mailing_street AS calle
# MAGIC          ,contacts.mailing_zip AS codigo_postal
# MAGIC          ,CASE 
# MAGIC               WHEN tablon.id_tipo_registro = 1 THEN tablon.nacionalidad
# MAGIC               WHEN tablon.id_tipo_registro IN (2,3) THEN contacts.nacionalidad
# MAGIC               ELSE NULL
# MAGIC              END nacionalidad
# MAGIC          ,tablon.fecha_hora_anulacion
# MAGIC          ,CASE WHEN tablon.cod_Owner = NULL OR tablon.cod_Owner = '' THEN 'n/a' ELSE tablon.cod_Owner END cod_Owner
# MAGIC          ,CASE WHEN tablon.nombre_estado_venta = NULL OR tablon.nombre_estado_venta = '' THEN 'n/a' ELSE tablon.nombre_estado_venta END nombre_estado_venta
# MAGIC          ,CASE WHEN tablon.etapa = NULL OR tablon.etapa = '' THEN 'n/a' ELSE tablon.etapa END etapa
# MAGIC          ,CASE WHEN tablon.cod_Producto = NULL OR tablon.cod_Producto = '' THEN 'n/a' ELSE tablon.cod_Producto END cod_Producto
# MAGIC          ,CASE WHEN tablon.tipo_Cliente_lead = NULL OR tablon.tipo_Cliente_lead = '' THEN 'n/a' ELSE tablon.tipo_Cliente_lead END tipo_Cliente_lead
# MAGIC          ,CASE WHEN tablon.residencia = NULL OR tablon.residencia = '' THEN 'n/a' ELSE tablon.residencia END residencia
# MAGIC          ,CASE WHEN tablon.motivo_Perdida = NULL OR tablon.motivo_Perdida = '' THEN 'n/a' ELSE tablon.motivo_Perdida END motivo_Perdida
# MAGIC          ,CASE WHEN tablon.utm_campaign_id = NULL OR tablon.utm_campaign_id = '' THEN 'n/a' ELSE tablon.utm_campaign_id END utm_campaign_id
# MAGIC          ,CASE WHEN tablon.utm_ad_id = NULL OR tablon.utm_ad_id = '' THEN 'n/a' ELSE tablon.utm_ad_id END utm_ad_id
# MAGIC          ,CASE WHEN tablon.utm_source = NULL OR tablon.utm_source = '' THEN 'n/a' ELSE tablon.utm_source END utm_source
# MAGIC      FROM silver_lakehouse.tablon_leads_and_deals tablon
# MAGIC LEFT JOIN silver_lakehouse.zohocontacts contacts
# MAGIC        ON tablon.cod_Oportunidad = contacts.id;
# MAGIC
# MAGIC select * from zoho_table_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW zoho_dimensions_temp AS
# MAGIC SELECT 
# MAGIC        tablon.id_tipo_registro
# MAGIC       ,tablon.tipo_registro
# MAGIC       ,tablon.cod_Lead
# MAGIC       ,tablon.cod_Oportunidad
# MAGIC       ,tablon.nombre
# MAGIC       ,tablon.email
# MAGIC       ,tablon.telefono
# MAGIC       ,tablon.nombre_contacto
# MAGIC       ,tablon.importe_venta
# MAGIC       ,tablon.importe_descuento
# MAGIC       ,tablon.importe_venta_neto
# MAGIC       ,estadoventa.id_dim_estado_venta
# MAGIC       ,etapaventa.id_dim_etapa_venta
# MAGIC       ,tablon.posibilidad_venta
# MAGIC       ,tablon.fec_creacion
# MAGIC       ,tablon.fec_modificacion
# MAGIC       ,tablon.fec_cierre
# MAGIC       ,tablon.fecha_hora_anulacion
# MAGIC       ,0 AS importe_matricula
# MAGIC       ,0 AS importe_descuento_matricula
# MAGIC       ,0 AS importe_neto_matricula
# MAGIC       ,tablon.ciudad AS ciudad
# MAGIC       ,tablon.provincia AS provincia
# MAGIC       ,tablon.calle AS calle
# MAGIC       ,tablon.codigo_postal AS codigo_postal
# MAGIC       ,tablon.fec_pago_matricula AS fec_pago_matricula
# MAGIC       ,tablon.nombre_scoring AS nombre_scoring
# MAGIC       ,tablon.puntos_scoring AS puntos_scoring
# MAGIC       ,tablon.dias_Cierre AS dias_cierre
# MAGIC       ,etapaventa.esNE AS kpi_new_enrollent
# MAGIC       ,perdida.esNeto AS kpi_lead_neto
# MAGIC       ,perdida.esBruto AS kpi_lead_bruto
# MAGIC       ,1 AS activo --PENDIENTE DEFINIR LOGICA POR PARTE DEL CLIENTE
# MAGIC       ,COALESCE(comercial.id_dim_comercial, -1) AS id_dim_propietario_lead
# MAGIC       ,COALESCE(programa.idDimPrograma, -1) AS id_pim_programa
# MAGIC       ,COALESCE(modalidad.id_dim_modalidad, -1) AS id_dim_modalidad
# MAGIC       ,COALESCE(institucion.id_dim_institucion, -1) AS id_dim_institucion
# MAGIC       ,COALESCE(sede.id_dim_sede, -1) AS id_dim_sede
# MAGIC       ,COALESCE(producto.idDimProducto, -1) AS id_dim_Producto
# MAGIC       ,COALESCE(formacion.id_dim_tipo_formacion, -1) AS id_dim_tipo_formacion
# MAGIC       ,COALESCE(tiponegocio.id_dim_tipo_negocio, -1) AS id_dim_tipo_negocio
# MAGIC       ,COALESCE(pais.id, -1) AS id_dim_pais
# MAGIC       ,perdida.idDimMotivoPerdida AS id_dim_motivo_perdida
# MAGIC       ,pais.id AS id_dim_nacionalidad
# MAGIC       ,utmcampaign.id_dim_utm_campaign AS id_dim_utm_campaign
# MAGIC       ,utmadset.id_dim_utm_ad AS id_dim_utm_ad
# MAGIC       ,utmsource.id_dim_utm_source AS id_dim_utm_source
# MAGIC FROM zoho_table_view tablon
# MAGIC LEFT JOIN gold_lakehouse.dim_comercial comercial ON tablon.cod_Owner = comercial.cod_comercial 
# MAGIC LEFT JOIN gold_lakehouse.dim_estado_venta estadoventa ON tablon.nombre_estado_venta = estadoventa.nombre_estado_venta
# MAGIC LEFT JOIN gold_lakehouse.dim_etapa_venta etapaventa ON tablon.etapa = etapaventa.nombre_etapa_venta
# MAGIC LEFT JOIN gold_lakehouse.dim_programa programa ON SUBSTRING(tablon.cod_Producto, 10, 5) = UPPER(programa.codPrograma)
# MAGIC LEFT JOIN gold_lakehouse.dim_modalidad modalidad ON SUBSTRING(tablon.cod_Producto, 18, 1) = modalidad.codigo
# MAGIC LEFT JOIN gold_lakehouse.dim_institucion institucion ON UPPER(programa.entidadLegal) = NULLIF(UPPER(institucion.nombre_institucion), '') 
# MAGIC LEFT JOIN gold_lakehouse.dim_sede sede ON SUBSTRING(tablon.cod_Producto, 20, 3) = NULLIF(sede.codigo_sede, '')
# MAGIC LEFT JOIN gold_lakehouse.dim_producto producto ON tablon.cod_Producto = NULLIF(producto.codProducto, '')
# MAGIC LEFT JOIN gold_lakehouse.dim_tipo_formacion formacion ON programa.tipoPrograma = NULLIF(formacion.tipo_formacion_desc, '')
# MAGIC LEFT JOIN gold_lakehouse.dim_tipo_negocio tiponegocio ON tablon.tipo_Cliente_lead = NULLIF(tiponegocio.tipo_negocio_desc, '')
# MAGIC LEFT JOIN gold_lakehouse.dim_pais pais ON UPPER(tablon.residencia) = NULLIF(UPPER(pais.nombre), '')
# MAGIC LEFT JOIN gold_lakehouse.dim_motivo_perdida perdida ON UPPER(tablon.motivo_Perdida) = NULLIF(UPPER(perdida.nombreDimMotivoPerdida), '')
# MAGIC LEFT JOIN gold_lakehouse.dim_utm_campaign utmcampaign ON tablon.utm_campaign_id = NULLIF(utmcampaign.utm_campaign_id, '')
# MAGIC LEFT JOIN gold_lakehouse.dim_utm_adset utmadset ON tablon.utm_ad_id = NULLIF(utmadset.utm_ad_id, '')
# MAGIC LEFT JOIN gold_lakehouse.dim_utm_source utmsource ON tablon.utm_source = NULLIF(utmsource.utm_source, '');
# MAGIC
# MAGIC select * from zoho_dimensions_temp;
# MAGIC

# COMMAND ----------

fct_venta_zoho_df = spark.sql("select * from zoho_dimensions_temp")

# COMMAND ----------

fct_venta_zoho_df.createOrReplaceTempView("fct_venta_view")

# COMMAND ----------

fct_venta_zoho_df = fct_venta_zoho_df.dropDuplicates()

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE WITH SCHEMA EVOLUTION 
# MAGIC INTO gold_lakehouse.fct_venta
# MAGIC USING fct_venta_view 
# MAGIC ON gold_lakehouse.fct_venta.cod_venta = fct_venta_view.cod_venta 
# MAGIC AND gold_lakehouse.fct_venta.sistema_origen = fct_venta_view.sistema_origen
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cod_venta, COUNT(*)
# MAGIC FROM fct_venta_view
# MAGIC GROUP BY cod_venta
# MAGIC HAVING COUNT(*) > 1;
