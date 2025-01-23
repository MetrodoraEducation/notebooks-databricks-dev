# Databricks notebook source
# MAGIC %md
# MAGIC **DB Silver**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS silver_lakehouse

# COMMAND ----------

# MAGIC %md
# MAGIC **Definición Storage Account**

# COMMAND ----------

storage_account_name = "stmetrodoralakehousedev"

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla Budget**

# COMMAND ----------


sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.budget
(
fecha date,
escenario string,
titulacion string,
centro string,
sede string,
modalidad string,
num_leads_netos integer,
num_leads_brutos integer,
new_enrollment integer,
importe_venta_neta double,
importe_venta_bruta double,
importe_captacion double,
processdate timestamp
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/budget';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **tabla aircallcalls**

# COMMAND ----------


sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.aircallcalls
(
country_code_a2 string,
direction string,
duration integer,
ended_at timestamp,
id string,
missed_call_reason string,
raw_digits string,
started_at timestamp,
processdate timestamp
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/aircallcalls';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla clientifydeals**

# COMMAND ----------


sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.clientifydeals
(
id string,
actual_closed_date timestamp,
amount double,
amount_user double,
company string,
company_name string,
contact string,
contact_email string,
contact_medium string,
contact_name string,
contact_phone string,
contact_source string,
created timestamp,
currency string,
deal_source string,
expected_closed_date timestamp,
lost_reason string,
modified timestamp,
name string,
owner string,
owner_name string,
pipeline string,
pipeline_desc string,
pipeline_stage string,
pipeline_stage_desc string,
probability integer,
probability_desc double,
source long,
status integer,
status_desc string,
custom_fields_byratings_rating string,
custom_fields_byratings_score double,
custom_fields_estudio_old string,
custom_fields_id string,
custom_fields_modalidad_old string,
custom_fields_sede_old string,
custom_fields_anio_academico string,
custom_fields_campaign_id string,
custom_fields_centro string,
custom_fields_ciudad string,
custom_fields_cp string,
custom_fields_curso_anio string,
custom_fields_descuento double,
custom_fields_descuento_matricula double,
custom_fields_estudio string,
custom_fields_fecha_inscripcion timestamp,
custom_fields_gclid string,
custom_fields_gdpr string,
custom_fields_google_id string,
custom_fields_linea_negocio string,
custom_fields_matricula double,
custom_fields_mensualidad double,
custom_fields_modalidad string,
custom_fields_pais string,
custom_fields_ref string,
custom_fields_sede string,
custom_fields_tipo_conversion string,
custom_fields_turno string,
custom_fields_ua string,
custom_fields_url string,
custom_fields_utm_ad_id string,
custom_fields_utm_adset_id string,
custom_fields_utm_campaign string,
custom_fields_utm_campaign_id string,
custom_fields_utm_campaign_name string,
custom_fields_utm_channel string,
custom_fields_utm_device string,
custom_fields_utm_estrategia string,
custom_fields_utm_medium string,
custom_fields_utm_network string,
custom_fields_utm_placement string,
custom_fields_utm_site_source_name string,
custom_fields_utm_source string,
custom_fields_utm_term string,
custom_fields_utm_type string,
processdate timestamp,
sourcesystem string
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/clientifydeals';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla odoolead**

# COMMAND ----------


sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.odoolead
(
id string,
campaign_id string,
city string,
contact_name string,
create_date string,
date_action_last timestamp,
date_closed timestamp,
date_conversion timestamp,
date_last_stage_update timestamp,
email_cc string,
email_from string,
medium_id string,
mobile string,
name string,
partner_name string,
phone string,
planned_revenue double,
probability double,
sale_amount_total double,
source_id string,
street string,
street2 string,
title string,
write_date timestamp,
x_codcurso string,
x_codmodalidad string,
x_curso string,
x_ga_campaign string,
x_ga_medium string,
x_ga_source string,
x_ga_utma string,
x_studio_field_fm3fx string,
zip string,
stage_id string,
stage_value string,
company_id string,
company_value string,
country_id string,
country_value string,
state_id string,
state_value string,
user_id string,
user_value string,
x_curso_id string,
x_curso_value string,
x_modalidad_id string,
x_modalidad_value string,
x_sede_id string,
x_sede_value string,
lost_reason_id string,
lost_reason_value string,
processdate timestamp,
sourcesystem string
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/odoolead';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla sales**

# COMMAND ----------


sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.sales
(
cod_venta string,
sistema_origen string,
fec_procesamiento TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/sales';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla clientifydealsidfordelete**

# COMMAND ----------


sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.clientifydealsidfordelete
(
id string
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/clientifydealsidfordelete';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dimPais**

# COMMAND ----------


sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.dim_pais
(
id int,
nombre string,
name string,
iso2 string,	
iso3 string
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_pais';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla Mapeo_Origen_Campania**

# COMMAND ----------


sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.mapeo_origen_campania
(
utm_source string, 
utm_type string, 
utm_channel string, 
utm_medium string 
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/mapeo_origen_campania';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla mapeo_modalidad**

# COMMAND ----------


sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.mapeo_modalidad
(
modalidad string, 
modalidad_norm string
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/mapeo_modalidad';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla mapeo_sede**

# COMMAND ----------


sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.mapeo_sede
(
sede string, 
sede_norm string
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/mapeo_sede';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla mapeo_estudio**

# COMMAND ----------


sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.mapeo_estudio
(
estudio string, 
estudio_norm string
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/mapeo_estudio';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_estudio**

# COMMAND ----------


sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.dim_estudio
(
cod_estudio string,
nombre_de_programa string,
cod_vertical string,
vertical_desc string,
cod_entidad_legal string,
entidad_legal_desc string,
cod_especialidad string,
especialidad_desc string,
cod_tipo_formacion string,
tipo_formacion_desc string,
cod_tipo_negocio string,
tipo_negocio_desc string
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_estudio';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,Table ZohoLeads

sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.zoholeads
(
id STRING,
first_name STRING,
last_name STRING,
email STRING,
mobile STRING,
modified_time TIMESTAMP,
lead_source STRING,
lead_status STRING,
lead_rating STRING,
lead_scoring STRING,
visitor_score STRING,
sexo STRING,
tipologia_cliente STRING,
type_conversion STRING,
residencia STRING,
provincia STRING,
motivos_perdida STRING,
nacionalidad STRING,
utm_source STRING,
utm_medium STRING,
utm_campaign_id STRING,
utm_campaign_name STRING,
utm_ad_id STRING,
utm_adset_id STRING,
utm_term STRING,
utm_channel STRING,
utm_type STRING,
utm_estrategia STRING,
google_click_id STRING,
facebook_click_id STRING,
id_producto STRING,
id_programa STRING,
id_correlacion_prospecto STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/zoholeads';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,Table ZohoDeals

sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.zohodeals
(
id STRING,
importe DOUBLE,
codigo_descuento STRING,
fecha_cierre DATE,
competencia STRING,
currency STRING,
deal_name STRING,
descuento DOUBLE,
exchange_rate DOUBLE,
fecha_hora_anulacion TIMESTAMP,
fecha_hora_documentacion_completada TIMESTAMP,
fecha_hora_pagado TIMESTAMP,
id_classlife STRING,
id_prospecto STRING,
id_producto STRING,
importe_pagado DOUBLE,
modified_time TIMESTAMP,
motivo_perdida_b2b STRING,
motivo_perdida_b2c STRING,
pipeline STRING,
probabilidad INT,
profesion_estudiante STRING,
residencia1 STRING,
etapa STRING,
tipologia_cliente STRING,
br_rating STRING,
br_score DOUBLE,
network STRING,
tipo_conversion STRING,
utm_ad_id STRING,
utm_adset_id STRING,
utm_campana_id STRING,
utm_campana_nombre STRING,
utm_canal STRING,
utm_estrategia STRING,
utm_medio STRING,
utm_perfil STRING,
utm_fuente STRING,
utm_termino STRING,
utm_tipo STRING,
processdate TIMESTAMP,
sourcesystem STRING,
tipo_cambio DOUBLE,
utm_campaign_id STRING,
utm_campaign_name STRING,
utm_channel STRING,
utm_strategy STRING,
utm_medium STRING,
utm_profile STRING,
utm_source STRING,
utm_term STRING,
utm_type STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/zohodeals';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,Table ZohoUsers

sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.zohousers
(
current_shift STRING,
next_shift STRING,
shift_effective_from STRING,
currency STRING,
isonline BOOLEAN,
modified_time TIMESTAMP,
alias STRING,
city STRING,
confirm BOOLEAN,
country STRING,
country_locale STRING,
created_time TIMESTAMP,
date_format STRING,
decimal_separator STRING,
dob STRING,
email STRING,
fax STRING,
first_name STRING,
full_name STRING,
id STRING,
language STRING,
last_name STRING,
locale STRING,
microsoft BOOLEAN,
mobile STRING,
number_separator STRING,
offset BIGINT,
phone STRING,
sandboxdeveloper BOOLEAN,
state STRING,
status_reason__s STRING,
street STRING,
time_format STRING,
time_zone STRING,
website STRING,
zip STRING,
zuid STRING,
modified_by_id STRING,
modified_by_name STRING,
created_by_id STRING,
created_by_name STRING,
processdate TIMESTAMP,
sourcesystem STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/zohousers';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,Table ZohoContacts

sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.zohocontacts
(
last_name STRING,
dni STRING,
date_birth STRING,
email STRING,
estudios STRING,
first_name STRING,
home_phone STRING,
id_classlife STRING,
mailing_city STRING,
mailing_country STRING,
mailing_street STRING,
mailing_zip STRING,
mobile STRING,
nacionalidad STRING,
other_city STRING,
other_country STRING,
other_state STRING,
other_street STRING,
other_zip STRING,
phone STRING,
profesion STRING,
provincia STRING,
residencia STRING,
secondary_email STRING,
sexo STRING,
tipo_cliente STRING,
tipo_contacto STRING,
id STRING,
recibir_comunicacion STRING,
woztellplatform_whatsapp_out BOOLEAN,
processdate TIMESTAMP,
sourcesystem STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/zohocontacts';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,Table ZohoCampaigns

sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.zohocampaigns
(
    actual_cost DOUBLE,
    budgeted_cost STRING,
    campaign_name STRING,
    currency STRING,
    description STRING,
    end_date STRING,
    exchange_rate LONG,
    expected_response STRING,
    expected_revenue STRING,
    business_line STRING,
    campaign_subject STRING,
    reply_to_address STRING,
    sender_address STRING,
    sender_name STRING,
    departmen_tid STRING,
    survey STRING,
    survey_department STRING,
    survey_type STRING,
    survey_url STRING,
    webinar_duration STRING,
    webinar_launch_url STRING,
    webinar_registration_url STRING,
    webinar_schedule STRING,
    num_sent STRING,
    parent_campaign STRING,
    start_date STRING,
    status STRING,
    type STRING,
    id STRING,
    created_by_email STRING,
    created_by_id STRING,
    created_by_name STRING,
    layout_id STRING,
    layout_name STRING,
    modified_by_email STRING,
    modified_by_id STRING,
    modified_by_name STRING,
    owner_email STRING,
    owner_id STRING,
    owner_name STRING,
    tag_color_code STRING,
    tag_id STRING,
    tag_name STRING,
    processDate TIMESTAMP,
    sourceSystem STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/zohocampaigns';
"""

# Ejecutar la consulta SQL con Spark
spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,Table ClasslifeTitulaciones

sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.classlifetitulaciones
(
    count BIGINT,
    limit BIGINT,
    page BIGINT,
    total BIGINT,
    ano_inicio_docencia STRING,
    certificado_euneiz_incluido STRING,
    cuotas_docencia STRING,
    entidad_legal STRING,
    entidad_legal_codigo STRING,
    fecha_fin_pago STRING,
    fecha_inicio_pago STRING,
    fecha_fin STRING,
    fecha_inicio STRING,
    grupo STRING,
    horas_acreditadas STRING,
    horas_presenciales STRING,
    mes_inicio_docencia STRING,
    meses_duracion STRING,
    modalidad STRING,
    no__ultimas_plazas STRING,
    tarifa_ampliacion BIGINT,
    tarifa_docencia BIGINT,
    tarifa_euneiz BIGINT,
    tarifa_matricula BIGINT,
    total_tarifas BIGINT,
    vertical STRING,
    acreditado STRING,
    admisionsino STRING,
    area_id BIGINT,
    area_title STRING,
    building_id STRING,
    building_title STRING,
    ciclo_id BIGINT,
    ciclo_title STRING,
    codigo_antiguo STRING,
    codigo_especialidad STRING,
    codigo_programa STRING,
    codigo_vertical STRING,
    creditos BIGINT,
    degree_id BIGINT,
    degree_title STRING,
    descripcion_calendario STRING,
    destinatarios STRING,
    enroll_alias STRING,
    enroll_end STRING,
    enroll_group_id BIGINT,
    enroll_group_name STRING,
    enroll_ini STRING,
    especialidad STRING,
    fecha_creacion STRING,
    modalidad_code STRING,
    nombre_antiguo_de_programa STRING,
    nombre_del_programa_oficial_completo STRING,
    nombreweb STRING,
    plan_id STRING,
    plan_title STRING,
    plazas BIGINT,
    school_id BIGINT,
    school_name STRING,
    section_id BIGINT,
    section_title STRING,
    term_id BIGINT,
    term_title STRING,
    tiponegocio STRING,
    ultima_actualizacion STRING,
    year BIGINT,
    counters_availables STRING,
    counters_enroll_group_id BIGINT,
    counters_enrolled STRING,
    counters_pre_enrolled STRING,
    counters_seats BIGINT,
    admisionsino_renamed_2 STRING,
    ano_inicio_docencia_renamed_2 STRING,
    building STRING,
    certificado_euneiz_incluido_renamed_2 STRING,
    codigo_entidad_legal STRING,
    codigo_modalidad STRING,
    codigo_sede STRING,
    codigo_vertical_renamed_2 STRING,
    descripcion_calendario_renamed_2 STRING,
    enroll_pago_ini_t STRING,
    excludesecurityarraymetas STRING,
    fecha_fin_cuotas STRING,
    fecha_fin_docencia STRING,
    fecha_fin_reconocimiento_ingresos STRING,
    fecha_inicio_cuotas STRING,
    fecha_inicio_docencia STRING,
    fecha_inicio_reconocimiento_ingresos STRING,
    grupo_renamed_2 STRING,
    grupos_cerrados STRING,
    horas_acreditadas_renamed_2 STRING,
    horas_presenciales_renamed_2 STRING,
    mes_inicio_docencia_renamed_2 STRING,
    mesesampliacion STRING,
    meses_cursos_open STRING,
    num_alumnos_inscritos STRING,
    num_plazas STRING,
    num_plazas_ultimas STRING,
    receipts_count STRING,
    roaster_ind STRING,
    tiponegocio_renamed_2 STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/classlifetitulaciones';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,Dimension Zoho
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.dim_zoho 
(
    lead_id STRING,
    contact_id STRING,
    opportunity_id STRING,
    owner_id STRING,
    lead_name STRING,
    lead_email STRING,
    lead_status STRING,
    lead_rating STRING,
    contact_name STRING,
    contact_email STRING,
    contact_phone STRING,
    opportunity_name STRING,
    opportunity_stage STRING,
    opportunity_probability INT,
    opportunity_amount DECIMAL(18,2),
    prioritized_field_1 STRING,
    prioritized_field_2 STRING,
    created_date TIMESTAMP,
    modified_date TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_zoho';
"""

# Ejecutar la consulta SQL con Spark
spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,Dimension Contact
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.dim_contact 
(
    contact_id STRING,
    contact_name STRING,
    contact_email STRING,
    contact_phone STRING,
    created_date TIMESTAMP,
    modified_date TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_contact';
"""

# Ejecutar la consulta SQL con Spark
spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,Dimension Opportunity
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.dim_opportunity 
(
    opportunity_id STRING,
    opportunity_name STRING,
    opportunity_stage STRING,
    opportunity_probability INT,
    opportunity_amount DECIMAL(18,2),
    created_date TIMESTAMP,
    modified_date TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_opportunity';
"""

# Ejecutar la consulta SQL con Spark
spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,Intemediate table lead_contact_mapping
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.lead_contact_mapping 
(
    lead_id STRING,
    contact_id STRING,
    mapping_date TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/lead_contact_mapping';
"""

# Ejecutar la consulta SQL con Spark
spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,Intermediate table lead_opportunity_mapping
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.lead_opportunity_mapping 
(
    lead_id STRING,
    opportunity_id STRING,
    mapping_date TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/lead_opportunity_mapping';
"""

# Ejecutar la consulta SQL con Spark
spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,Intermediate table campaign_lead_mapping
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.campaign_lead_mapping 
(
    campaign_id STRING,
    lead_id STRING,
    mapping_date TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/campaign_lead_mapping';
"""

# Ejecutar la consulta SQL con Spark
spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,Dimension Lead
#Crear en Gold
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_lead 
(
    lead_id STRING,
    lead_name STRING,
    lead_email STRING,
    lead_status STRING,
    lead_rating STRING,
    created_date TIMESTAMP,
    modified_date TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_lead';
"""

# Ejecutar la consulta SQL con Spark
spark.sql(sql_query)
