# Databricks notebook source
# MAGIC %md
# MAGIC **Database Gold**

# COMMAND ----------

# DBTITLE 1,Create database gold_lakehouse
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold_lakehouse

# COMMAND ----------

# MAGIC %md
# MAGIC **Definición Storage Account**

# COMMAND ----------

# DBTITLE 1,Declare variable storage_account_name
storage_account_name = "stmetrodoralakehousedev"

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla fct_llamada**

# COMMAND ----------

# DBTITLE 1,Create or replace fct_llamada

sql_query = f"""
CREATE OR REPLACE TABLE gold_lakehouse.fct_llamada (
    id_llamada BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 0 INCREMENT BY 1), 
    cod_llamada STRING,
    id_dim_tiempo DATE,
    id_dim_hora STRING,
    direccion_llamada STRING,
    duration INT,
    numero_telefono STRING,
    id_dim_pais INT,
    id_dim_comercial INT,
    id_dim_motivo_perdida_llamada INT,
    processdate TIMESTAMP
)   
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/fct_llamada';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_fecha**

# COMMAND ----------

# DBTITLE 1,Create or replace dim_fecha

sql_query = f"""
CREATE OR REPLACE TABLE gold_lakehouse.dim_fecha (
    id_dim_fecha DATE, 
    fecha_larga DATE, 
    dia_semana STRING,
    dia_semana_corto STRING, 
    dia_semana_numero INT, 
    mes_numero INT, 
    mes_corto STRING, 
    mes_largo STRING, 
    dia_anio INT,
    semana_anio INT,
    numero_dias_mes INT, 
    primer_dia_mes DATE, 
    ultimo_dia_mes DATE, 
    anio_numero INT,
    trimestre_numero INT, 
    trimestre_nombre STRING, 
    mes_fiscal_numero INT, 
    anio_fiscal_numero INT,
    curso_academico STRING, 
    es_laborable BOOLEAN, 
    es_finde_semana BOOLEAN 
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_fecha';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_hora**

# COMMAND ----------

# DBTITLE 1,Create or replace dim_hora

sql_query = f"""
CREATE OR REPLACE TABLE gold_lakehouse.dim_hora (
    id_dim_hora INT, 
    id_dim_hora_larga STRING,
    hora INT, 
    minuto INT, 
    hora12 INT, 
    pmam STRING 
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_hora';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_pais**

# COMMAND ----------

# DBTITLE 1,Create or replace dim_pais

sql_query = f"""
CREATE OR REPLACE TABLE gold_lakehouse.dim_pais (
    id int,
    nombre string,
    name string,
    nombre_nacionalidad string,
    iso2 string,	
    iso3 string
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_pais';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_nacionalidad**

# COMMAND ----------


sql_query = f"""
CREATE OR REPLACE TABLE gold_lakehouse.dim_nacionalidad (
    id int,
    nombre string,
    name string,
    nombre_nacionalidad string,
    iso2 string,	
    iso3 string
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_nacionalidad';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_motivo_perdida_llamada**

# COMMAND ----------

# DBTITLE 1,Create or replace dim_motivo_perdida_llamada

sql_query = f"""
CREATE OR REPLACE TABLE gold_lakehouse.dim_motivo_perdida_llamada (
    id_dim_motivo_perdida_llamada BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),    
    motivo_perdida_llamada STRING,
    tipo_perdida STRING
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_motivo_perdida_llamada';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_comercial**

# COMMAND ----------

# DBTITLE 1,Create or replace dim_comercial

sql_query = f"""
CREATE OR REPLACE TABLE gold_lakehouse.dim_comercial (
    id_dim_comercial BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    cod_comercial STRING, 
    nombre_comercial STRING,
    equipo_comercial STRING,    
    activo INTEGER,
    fecha_desde DATE,
    fecha_hasta DATE
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_comercial';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla Mapeo Origen _Campania**

# COMMAND ----------

# DBTITLE 1,mapeo_origen_campania

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.mapeo_origen_campania
(
utm_source string, 
utm_type string, 
utm_channel string, 
utm_medium string 
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/mapeo_origen_campania';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_origen_campania**

# COMMAND ----------

# DBTITLE 1,dim_origen_campania
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_origen_campania
(
    id_dim_origen_campania BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    nombre_origen_campania string, 
    tipo_campania string, 
    canal_campania string, 
    medio_campania string,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP,
    fec_procesamiento TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_origen_campania';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_campania**

# COMMAND ----------

# DBTITLE 1,dim_campania

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_campania
(
id_dim_campania BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
nombre_campania string
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_campania';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_estado_venta**

# COMMAND ----------

# DBTITLE 1,dim_estado_venta

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_estado_venta
(
    id_dim_estado_venta BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    nombre_estado_venta STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_estado_venta';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_etapa_venta**

# COMMAND ----------

# DBTITLE 1,dim_etapa_venta

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_etapa_venta
(
    id_dim_etapa_venta BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    nombre_etapa_venta STRING,
    nombreEtapaVentaAgrupado STRING,
    esNe BIGINT,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_etapa_venta';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla mapeo_modalidad**

# COMMAND ----------

# DBTITLE 1,mapeo_modalidad

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.mapeo_modalidad
(
modalidad string, 
modalidad_norm string
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/mapeo_modalidad';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_modalidad**

# COMMAND ----------

# DBTITLE 1,dim_modalidad

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_modalidad
(
        id_dim_modalidad BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
        nombre_modalidad STRING,
        codigo STRING,
        ETLcreatedDate TIMESTAMP,
        ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_modalidad';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_institucion**

# COMMAND ----------

# DBTITLE 1,dim_institucion

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_institucion
(
    id_dim_institucion BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    nombre_institucion STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_institucion';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla mapeo_sede**

# COMMAND ----------

# DBTITLE 1,mapeo_sede

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.mapeo_sede
(
sede string, 
sede_norm string
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/mapeo_sede';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_sede**

# COMMAND ----------

# DBTITLE 1,dim_sede

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_sede
(
    id_dim_sede BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    nombre_sede STRING,
    codigo_sede STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_sede';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla mapeo_estudio**

# COMMAND ----------

# DBTITLE 1,mapeo_estudio

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.mapeo_estudio
(
estudio string, 
estudio_norm string
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/mapeo_estudio';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_estudio**

# COMMAND ----------

# DBTITLE 1,dim_estudio

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_estudio
(
    id_dim_estudio BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
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
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_estudio';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_localidad**

# COMMAND ----------

# DBTITLE 1,dim_localidad

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_localidad
(
    id_dim_localidad BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    nombre_localidad STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_localidad';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_motivo_cierre**

# COMMAND ----------

# DBTITLE 1,dim_motivo_cierre

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_motivo_cierre
(
    id_dim_motivo_cierre BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    motivo_cierre STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_motivo_cierre';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_tipo_formacion**

# COMMAND ----------

# DBTITLE 1,dim_tipo_formacion

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_tipo_formacion
(
    id_dim_tipo_formacion BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    tipo_formacion_desc STRING,
    cod_tipo_formacion STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_tipo_formacion';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_tipo_negocio**

# COMMAND ----------

# DBTITLE 1,dim_tipo_negocio

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_tipo_negocio
(
    id_dim_tipo_negocio BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    tipo_negocio_desc STRING,
    cod_tipo_negocio STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_tipo_negocio';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla fct_venta**

# COMMAND ----------

# DBTITLE 1,fct_venta

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.fct_venta
(
    id_venta BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    cod_venta string,
    nombre string,
    email string,
    telefono string,
    nombre_contacto string,
    id_dim_propietario_lead BIGINT,
    id_dim_origen_campania BIGINT,
    id_dim_campania BIGINT,
    importe_venta double,
    importe_descuento double,
    importe_venta_neta double,
    id_dim_estado_venta BIGINT,
    id_dim_etapa_venta BIGINT,
    posibilidad_venta double,
    fec_creacion date,
    fec_modificacion date,
    fec_cierre date,
    id_dim_modalidad BIGINT,
    id_dim_institucion BIGINT,
    id_dim_sede BIGINT,
    id_dim_pais INT,
    id_dim_estudio BIGINT,
    fec_pago_matricula date,
    importe_matricula double, 
    importe_descuento_matricula double,
    importe_neto_matricula double,
    id_dim_localidad BIGINT,
    id_dim_tipo_formacion BIGINT,
    id_dim_tipo_negocio BIGINT,
    nombre_scoring string,
    puntos_scoring double,
    dias_cierre int,
    id_dim_motivo_cierre BIGINT,
    fec_procesamiento timestamp,
    sistema_origen string,
    tiempo_de_maduracion int,
    new_enrollent INT,
    lead_neto INT,
    activo INT
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/fct_venta';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_escenario_budget**

# COMMAND ----------

# DBTITLE 1,dim_escenario_budget

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_escenario_budget
(
id_dim_escenario_budget BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
escenario string
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_escenario_budget';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_titulacion_budget**

# COMMAND ----------

# DBTITLE 1,dim_titulacion_budget

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_titulacion_budget
(
id_dim_titulacion_budget BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
titulacion string
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_titulacion_budget';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla fct_budget**

# COMMAND ----------

# DBTITLE 1,fct_budget

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.fct_budget
(
id_budget BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
fec_budget date,
id_dim_escenario_budget	BIGINT,
id_dim_titulacion_budget BIGINT,
centro string,
sede string,
modalidad string,	
num_leads_netos	integer,
num_leads_brutos integer,
num_matriculas integer,
importe_venta_neta double,
importe_venta_bruta double,
importe_captacion	double,
fec_procesamiento timestamp
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/fct_budget';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,dim_producto
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_producto
(
    idDimProducto BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    codProductoOrigen STRING,
    codProductoCorto STRING,
    codProducto STRING NOT NULL,
    origenProducto STRING,
    tipoProducto STRING,
    area STRING,
    nombreOficial STRING,
    curso STRING,
    numeroCurso INT,
    fechaInicioCurso DATE,
    fechaFinCurso DATE,
    ciclo_id INT,
    numPlazas INT,
    numGrupo INT,
    vertical STRING,
    codVertical STRING,
    especialidad STRING,
    codEspecialidad STRING,
    numCreditos DOUBLE,
    codPrograma STRING,
    admiteAdmision STRING,
    tipoNegocio STRING,
    acreditado STRING,
    nombreWeb STRING,
    entidadLegal STRING,
    codEntidadLegal STRING,
    modalidad STRING,
    codModalidad STRING,
    fechaInicio DATE,
    fechaFin DATE,
    mesesDuracion INT,
    horasAcreditadas INT,
    horasPresenciales INT,
    fechaInicioPago DATE,
    fechaFinPago DATE,
    numCuotas INT,
    importeCertificado DOUBLE,
    importeAmpliacion DOUBLE,
    importeDocencia DOUBLE,
    importeMatricula DOUBLE,
    importeTotal DOUBLE,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_producto';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,dim_programa
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_programa
(
    idDimPrograma BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    codPrograma STRING NOT NULL,
    nombrePrograma STRING NOT NULL,
    tipoPrograma STRING,
    entidadLegal STRING,
    especialidad STRING,
    vertical STRING,
    nombreProgramaCompleto STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_programa';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,dim_vertical
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_vertical 
(
    idDimVertical BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    nombreVertical STRING,
    nombreVerticalCorto STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_vertical ';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,dim_entidad_legal
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_entidad_legal 
(
    idDimInstitucion BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    nombreInstitucion STRING NOT NULL,
    codigoEntidadLegal STRING NOT NULL,  -- Aquí aplicamos NOT NULL desde el inicio
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_entidad_legal ';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,dim_motivo_perdida
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_motivo_perdida 
(
    idDimMotivoPerdida BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    nombreDimMotivoPerdida STRING,
    cantidad STRING,
    calidad STRING,
    esBruto BIGINT,
    esNeto BIGINT,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_motivo_perdida';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,dim_especialidad
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_especialidad 
(
    idDimEspecialidad BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    nombreEspecialidad STRING NOT NULL,
    codEspecialidad STRING NOT NULL,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_especialidad';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,dim_utm_campaign
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_utm_campaign 
(
    id_dim_utm_campaign BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1) PRIMARY KEY,
    utm_campaign_id STRING NOT NULL,
    utm_campaign_name STRING,
    utm_strategy STRING,
    utm_channel STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_utm_campaign';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,dim_utm_adset
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_utm_adset 
(
    id_dim_utm_ad BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1) PRIMARY KEY,
    utm_ad_id STRING NOT NULL,
    utm_adset_id STRING,
    utm_term STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_utm_adset';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,dim_utm_source
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_utm_source 
(
    id_dim_utm_source BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1) PRIMARY KEY,
    utm_source STRING NOT NULL,
    utm_type STRING,
    utm_medium STRING,
    utm_profile STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_utm_source';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,fctventa Zoho

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.fctventa
(
    id_venta BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    cod_lead BIGINT,
    cod_oportunidad BIGINT,
    nombre string,
    email string,
    telefono string,
    nombre_contacto string,
    id_dim_propietario_lead BIGINT,
    importe_venta double,
    importe_descuento double,
    importe_venta_neta double,
    id_dim_estado_venta BIGINT,
    id_dim_etapa_venta BIGINT,
    posibilidad_venta double,
    fec_creacion date,
    fec_modificacion date,
    fec_cierre date,
    id_dim_modalidad BIGINT,
    id_dim_institucion BIGINT,
    id_dim_sede BIGINT,
    id_dim_pais INT,
    fec_pago_matricula date,
    importe_matricula double,
    importe_descuento_matricula double,
    importe_neto_matricula double,
    ciudad string,
    provincia string,
    calle string,
    codigo_postal string,
    id_dim_programa BIGINT,
    id_dim_producto BIGINT,
    id_dim_utm_campaign BIGINT,
    id_dim_utm_ad BIGINT,
    id_dim_utm_source BIGINT,
    id_dim_nacionalidad BIGINT,
    id_dim_tipo_formacion BIGINT,
    id_dim_tipo_negocio BIGINT,
    nombre_scoring string,
    puntos_scoring double,
    dias_cierre int,
    fecAnulacion TIMESTAMP,
    id_dim_motivo_perdida BIGINT,
    kpi_new_enrollent BIGINT,
    kpi_lead_neto BIGINT,
    kpi_lead_bruto BIGINT,
    activo BIGINT,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/fctventa';
"""

spark.sql(sql_query)
