# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_PRODUCTO**

# COMMAND ----------

# MAGIC %md
# MAGIC El código está preparando y limpiando datos relacionados con productos educativos antes de insertarlos en una dimensión de productos dentro de un sistema tipo Data Lakehouse. 
# MAGIC
# MAGIC Se centra en:
# MAGIC Asignación de claves de productos
# MAGIC Normalización y validación de datos
# MAGIC Conversión de formatos incorrectos
# MAGIC Creación de una vista que sirva como base para operaciones posteriores

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_producto_view AS
# MAGIC SELECT DISTINCT
# MAGIC     enroll_group_id AS codProductoOrigen,
# MAGIC     enroll_alias AS codProductoCorto,
# MAGIC     enroll_group_name AS codProducto,
# MAGIC     school_name AS origenProducto,
# MAGIC     degree_title AS tipoProducto,
# MAGIC     area_title AS area,
# MAGIC     nombre_del_programa_oficial_completo AS nombreOficial,
# MAGIC     ciclo_title AS curso,
# MAGIC     -- Manejo de valores vacíos en INT
# MAGIC     CASE 
# MAGIC         WHEN TRIM(year) = '' THEN NULL
# MAGIC         ELSE TRY_CAST(year AS INT) 
# MAGIC     END AS numeroCurso,
# MAGIC     -- Manejo de valores inválidos en fechas
# MAGIC     CASE 
# MAGIC         WHEN enroll_ini IN ('', '00/00/0000', '0000-00-00') THEN NULL 
# MAGIC         ELSE TRY_CAST(enroll_ini AS DATE) 
# MAGIC     END AS fechaInicioCurso,
# MAGIC     CASE 
# MAGIC         WHEN enroll_end IN ('', '00/00/0000', '0000-00-00') THEN NULL 
# MAGIC         ELSE TRY_CAST(enroll_end AS DATE) 
# MAGIC     END AS fechaFinCurso,
# MAGIC     CASE 
# MAGIC         WHEN TRIM(ciclo_id) = '' THEN NULL
# MAGIC         ELSE TRY_CAST(ciclo_id AS INT) 
# MAGIC     END AS ciclo_id,
# MAGIC     CASE 
# MAGIC         WHEN TRIM(plazas) = '' THEN 0
# MAGIC         ELSE TRY_CAST(plazas AS INT) 
# MAGIC     END AS numPlazas,
# MAGIC     CASE 
# MAGIC         WHEN TRIM(grupo) = '' THEN 0
# MAGIC         ELSE TRY_CAST(grupo AS INT) 
# MAGIC     END AS numGrupo,
# MAGIC     vertical,
# MAGIC     codigo_vertical AS codVertical,
# MAGIC     especialidad,
# MAGIC     codigo_especialidad AS codEspecialidad,
# MAGIC     CASE 
# MAGIC         WHEN TRIM(creditos) = '' THEN 0
# MAGIC         ELSE TRY_CAST(creditos AS DOUBLE) 
# MAGIC     END AS numCreditos,
# MAGIC     codigo_programa AS codPrograma,
# MAGIC     admisionsino AS admiteAdmision,
# MAGIC     tiponegocio AS tipoNegocio,
# MAGIC     acreditado,
# MAGIC     nombreweb AS nombreWeb,
# MAGIC     entidad_legal AS entidadLegal,
# MAGIC     codigo_entidad_legal AS codEntidadLegal,
# MAGIC     modalidad,
# MAGIC     codigo_modalidad AS codModalidad,
# MAGIC     -- Fechas con manejo de valores vacíos
# MAGIC     CASE 
# MAGIC         WHEN fecha_inicio IN ('', '00/00/0000', '0000-00-00') THEN NULL 
# MAGIC         ELSE TRY_CAST(fecha_inicio AS DATE) 
# MAGIC     END AS fechaInicio,
# MAGIC     CASE 
# MAGIC         WHEN fecha_fin IN ('', '00/00/0000', '0000-00-00') THEN NULL 
# MAGIC         ELSE TRY_CAST(fecha_fin AS DATE) 
# MAGIC     END AS fechaFin,
# MAGIC     CASE 
# MAGIC         WHEN TRIM(meses_duracion) = '' THEN 0
# MAGIC         ELSE TRY_CAST(meses_duracion AS INT) 
# MAGIC     END AS mesesDuracion,
# MAGIC     CASE 
# MAGIC         WHEN TRIM(horas_acreditadas) = '' THEN 0
# MAGIC         ELSE TRY_CAST(horas_acreditadas AS INT) 
# MAGIC     END AS horasAcreditadas,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN TRIM(horas_presenciales) = '' THEN 0
# MAGIC         ELSE TRY_CAST(horas_presenciales AS INT) 
# MAGIC     END AS horasPresenciales,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN fecha_inicio_pago IN ('', '00/00/0000', '0000-00-00') THEN NULL 
# MAGIC         ELSE TRY_CAST(fecha_inicio_pago AS DATE) 
# MAGIC     END AS fechaInicioPago,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN fecha_fin_pago IN ('', '00/00/0000', '0000-00-00') THEN NULL 
# MAGIC         ELSE TRY_CAST(fecha_fin_pago AS DATE) 
# MAGIC     END AS fechaFinPago,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN TRIM(cuotas_docencia) = '' THEN 0
# MAGIC         ELSE TRY_CAST(cuotas_docencia AS INT) 
# MAGIC     END AS numCuotas,
# MAGIC
# MAGIC     -- Valores numéricos con manejo de NULLs
# MAGIC     CASE 
# MAGIC         WHEN TRIM(tarifa_euneiz) = '' THEN 0.0
# MAGIC         ELSE TRY_CAST(tarifa_euneiz AS DOUBLE) 
# MAGIC     END AS importeCertificado,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN TRIM(tarifa_ampliacion) = '' THEN 0.0
# MAGIC         ELSE TRY_CAST(tarifa_ampliacion AS DOUBLE) 
# MAGIC     END AS importeAmpliacion,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN TRIM(tarifa_docencia) = '' THEN 0.0
# MAGIC         ELSE TRY_CAST(tarifa_docencia AS DOUBLE) 
# MAGIC     END AS importeDocencia,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN TRIM(tarifa_matricula) = '' THEN 0.0
# MAGIC         ELSE TRY_CAST(tarifa_matricula AS DOUBLE) 
# MAGIC     END AS importeMatricula,
# MAGIC
# MAGIC     -- Cálculo total con manejo de NULLs
# MAGIC     COALESCE(
# MAGIC         TRY_CAST(tarifa_docencia AS DOUBLE), 0.0
# MAGIC     ) + COALESCE(
# MAGIC         TRY_CAST(tarifa_matricula AS DOUBLE), 0.0
# MAGIC     ) AS importeTotal,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN fecha_creacion IN ('', '00/00/0000', '0000-00-00') THEN NULL 
# MAGIC         ELSE TRY_CAST(fecha_creacion AS TIMESTAMP) 
# MAGIC     END AS ETLcreatedDate,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN ultima_actualizacion IN ('', '00/00/0000', '0000-00-00') THEN NULL 
# MAGIC         ELSE TRY_CAST(ultima_actualizacion AS TIMESTAMP) 
# MAGIC     END AS ETLupdatedDate,
# MAGIC     current_timestamp() AS created_at,
# MAGIC     current_timestamp() AS updated_at
# MAGIC FROM silver_lakehouse.classlifetitulaciones;
# MAGIC
# MAGIC select * from dim_producto_view
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_producto_view limit 25

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_producto AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT
# MAGIC         codProductoOrigen,
# MAGIC         codProductoCorto,
# MAGIC         codProducto,
# MAGIC         origenProducto,
# MAGIC         tipoProducto,
# MAGIC         area,
# MAGIC         nombreOficial,
# MAGIC         curso,
# MAGIC         numeroCurso,
# MAGIC         fechaInicioCurso,
# MAGIC         fechaFinCurso,
# MAGIC         ciclo_id,
# MAGIC         numPlazas,
# MAGIC         numGrupo,
# MAGIC         vertical,
# MAGIC         codVertical,
# MAGIC         especialidad,
# MAGIC         codEspecialidad,
# MAGIC         numCreditos,
# MAGIC         codPrograma,
# MAGIC         admiteAdmision,
# MAGIC         tipoNegocio,
# MAGIC         acreditado,
# MAGIC         nombreWeb,
# MAGIC         entidadLegal,
# MAGIC         codEntidadLegal,
# MAGIC         modalidad,
# MAGIC         codModalidad,
# MAGIC         fechaInicio,
# MAGIC         fechaFin,
# MAGIC         mesesDuracion,
# MAGIC         horasAcreditadas,
# MAGIC         horasPresenciales,
# MAGIC         fechaInicioPago,
# MAGIC         fechaFinPago,
# MAGIC         numCuotas,
# MAGIC         importeCertificado,
# MAGIC         importeAmpliacion,
# MAGIC         importeDocencia,
# MAGIC         importeMatricula,
# MAGIC         importeTotal,
# MAGIC         CURRENT_TIMESTAMP AS ETLupdatedDate,
# MAGIC         COALESCE(ETLcreatedDate, CURRENT_TIMESTAMP) AS ETLcreatedDate
# MAGIC     FROM dim_producto_view
# MAGIC     WHERE codProductoOrigen IS NOT NULL
# MAGIC ) AS source
# MAGIC ON target.codProductoOrigen = source.codProductoOrigen
# MAGIC
# MAGIC -- Si los datos han cambiado, actualizamos todas las columnas
# MAGIC WHEN MATCHED AND (
# MAGIC     target.codProductoCorto <> source.codProductoCorto OR
# MAGIC     target.codProducto <> source.codProducto OR
# MAGIC     target.origenProducto <> source.origenProducto OR
# MAGIC     target.tipoProducto <> source.tipoProducto OR
# MAGIC     target.area <> source.area OR
# MAGIC     target.nombreOficial <> source.nombreOficial OR
# MAGIC     target.curso <> source.curso OR
# MAGIC     target.numeroCurso <> source.numeroCurso OR
# MAGIC     target.fechaInicioCurso <> source.fechaInicioCurso OR
# MAGIC     target.fechaFinCurso <> source.fechaFinCurso OR
# MAGIC     target.ciclo_id <> source.ciclo_id OR
# MAGIC     target.numPlazas <> source.numPlazas OR
# MAGIC     target.numGrupo <> source.numGrupo OR
# MAGIC     target.vertical <> source.vertical OR
# MAGIC     target.codVertical <> source.codVertical OR
# MAGIC     target.especialidad <> source.especialidad OR
# MAGIC     target.codEspecialidad <> source.codEspecialidad OR
# MAGIC     target.numCreditos <> source.numCreditos OR
# MAGIC     target.codPrograma <> source.codPrograma OR
# MAGIC     target.admiteAdmision <> source.admiteAdmision OR
# MAGIC     target.tipoNegocio <> source.tipoNegocio OR
# MAGIC     target.acreditado <> source.acreditado OR
# MAGIC     target.nombreWeb <> source.nombreWeb OR
# MAGIC     target.entidadLegal <> source.entidadLegal OR
# MAGIC     target.codEntidadLegal <> source.codEntidadLegal OR
# MAGIC     target.modalidad <> source.modalidad OR
# MAGIC     target.codModalidad <> source.codModalidad OR
# MAGIC     target.fechaInicio <> source.fechaInicio OR
# MAGIC     target.fechaFin <> source.fechaFin OR
# MAGIC     target.mesesDuracion <> source.mesesDuracion OR
# MAGIC     target.horasAcreditadas <> source.horasAcreditadas OR
# MAGIC     target.horasPresenciales <> source.horasPresenciales OR
# MAGIC     target.fechaInicioPago <> source.fechaInicioPago OR
# MAGIC     target.fechaFinPago <> source.fechaFinPago OR
# MAGIC     target.numCuotas <> source.numCuotas OR
# MAGIC     target.importeCertificado <> source.importeCertificado OR
# MAGIC     target.importeAmpliacion <> source.importeAmpliacion OR
# MAGIC     target.importeDocencia <> source.importeDocencia OR
# MAGIC     target.importeMatricula <> source.importeMatricula OR
# MAGIC     target.importeTotal <> source.importeTotal
# MAGIC ) THEN
# MAGIC UPDATE SET
# MAGIC     target.codProductoCorto = source.codProductoCorto,
# MAGIC     target.codProducto = source.codProducto,
# MAGIC     target.origenProducto = source.origenProducto,
# MAGIC     target.tipoProducto = source.tipoProducto,
# MAGIC     target.area = source.area,
# MAGIC     target.nombreOficial = source.nombreOficial,
# MAGIC     target.curso = source.curso,
# MAGIC     target.numeroCurso = source.numeroCurso,
# MAGIC     target.fechaInicioCurso = source.fechaInicioCurso,
# MAGIC     target.fechaFinCurso = source.fechaFinCurso,
# MAGIC     target.ciclo_id = source.ciclo_id,
# MAGIC     target.numPlazas = source.numPlazas,
# MAGIC     target.numGrupo = source.numGrupo,
# MAGIC     target.vertical = source.vertical,
# MAGIC     target.codVertical = source.codVertical,
# MAGIC     target.especialidad = source.especialidad,
# MAGIC     target.codEspecialidad = source.codEspecialidad,
# MAGIC     target.numCreditos = source.numCreditos,
# MAGIC     target.codPrograma = source.codPrograma,
# MAGIC     target.admiteAdmision = source.admiteAdmision,
# MAGIC     target.tipoNegocio = source.tipoNegocio,
# MAGIC     target.acreditado = source.acreditado,
# MAGIC     target.nombreWeb = source.nombreWeb,
# MAGIC     target.entidadLegal = source.entidadLegal,
# MAGIC     target.codEntidadLegal = source.codEntidadLegal,
# MAGIC     target.modalidad = source.modalidad,
# MAGIC     target.codModalidad = source.codModalidad,
# MAGIC     target.fechaInicio = source.fechaInicio,
# MAGIC     target.fechaFin = source.fechaFin,
# MAGIC     target.mesesDuracion = source.mesesDuracion,
# MAGIC     target.horasAcreditadas = source.horasAcreditadas,
# MAGIC     target.horasPresenciales = source.horasPresenciales,
# MAGIC     target.fechaInicioPago = source.fechaInicioPago,
# MAGIC     target.fechaFinPago = source.fechaFinPago,
# MAGIC     target.numCuotas = source.numCuotas,
# MAGIC     target.importeCertificado = source.importeCertificado,
# MAGIC     target.importeAmpliacion = source.importeAmpliacion,
# MAGIC     target.importeDocencia = source.importeDocencia,
# MAGIC     target.importeMatricula = source.importeMatricula,
# MAGIC     target.importeTotal = source.importeTotal,
# MAGIC     target.ETLupdatedDate = source.ETLupdatedDate
# MAGIC
# MAGIC -- Insertar nuevos registros si no existen
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (
# MAGIC     codProductoOrigen, codProductoCorto, codProducto, origenProducto, tipoProducto, area, nombreOficial, curso, numeroCurso, 
# MAGIC     fechaInicioCurso, fechaFinCurso, ciclo_id, numPlazas, numGrupo, vertical, codVertical, especialidad, codEspecialidad, numCreditos, 
# MAGIC     codPrograma, admiteAdmision, tipoNegocio, acreditado, nombreWeb, entidadLegal, codEntidadLegal, modalidad, codModalidad, fechaInicio, 
# MAGIC     fechaFin, mesesDuracion, horasAcreditadas, horasPresenciales, fechaInicioPago, fechaFinPago, numCuotas, importeCertificado, 
# MAGIC     importeAmpliacion, importeDocencia, importeMatricula, importeTotal, ETLcreatedDate, ETLupdatedDate
# MAGIC ) VALUES (
# MAGIC     source.codProductoOrigen, source.codProductoCorto, source.codProducto, source.origenProducto, source.tipoProducto, source.area, 
# MAGIC     source.nombreOficial, source.curso, source.numeroCurso, source.fechaInicioCurso, source.fechaFinCurso, source.ciclo_id, source.numPlazas, 
# MAGIC     source.numGrupo, source.vertical, source.codVertical, source.especialidad, source.codEspecialidad, source.numCreditos, source.codPrograma, 
# MAGIC     source.admiteAdmision, source.tipoNegocio, source.acreditado, source.nombreWeb, source.entidadLegal, source.codEntidadLegal, source.modalidad, 
# MAGIC     source.codModalidad, source.fechaInicio, source.fechaFin, source.mesesDuracion, source.horasAcreditadas, source.horasPresenciales, 
# MAGIC     source.fechaInicioPago, source.fechaFinPago, source.numCuotas, source.importeCertificado, source.importeAmpliacion, source.importeDocencia, 
# MAGIC     source.importeMatricula, source.importeTotal, source.ETLcreatedDate, source.ETLupdatedDate
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_lakehouse.dim_producto LIMIT 100;

# COMMAND ----------

# DBTITLE 1,Validate duplicates >1
# MAGIC %sql
# MAGIC SELECT codProductoorigen, COUNT(*) AS total_duplicados
# MAGIC FROM gold_lakehouse.dim_producto
# MAGIC GROUP BY codProductoorigen
# MAGIC HAVING COUNT(*) > 1;
