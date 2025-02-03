# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_PRODUCTO**

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
# MAGIC
# MAGIC     -- Manejo de valores vacíos en INT
# MAGIC     CASE 
# MAGIC         WHEN TRIM(year) = '' THEN NULL
# MAGIC         ELSE TRY_CAST(year AS INT) 
# MAGIC     END AS numeroCurso,
# MAGIC
# MAGIC     -- Manejo de valores inválidos en fechas
# MAGIC     CASE 
# MAGIC         WHEN enroll_ini IN ('', '00/00/0000', '0000-00-00') THEN NULL 
# MAGIC         ELSE TRY_CAST(enroll_ini AS DATE) 
# MAGIC     END AS fechaInicioCurso,
# MAGIC     
# MAGIC     CASE 
# MAGIC         WHEN enroll_end IN ('', '00/00/0000', '0000-00-00') THEN NULL 
# MAGIC         ELSE TRY_CAST(enroll_end AS DATE) 
# MAGIC     END AS fechaFinCurso,
# MAGIC     
# MAGIC     CASE 
# MAGIC         WHEN TRIM(ciclo_id) = '' THEN NULL
# MAGIC         ELSE TRY_CAST(ciclo_id AS INT) 
# MAGIC     END AS ciclo_id,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN TRIM(plazas) = '' THEN 0
# MAGIC         ELSE TRY_CAST(plazas AS INT) 
# MAGIC     END AS numPlazas,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN TRIM(grupo) = '' THEN 0
# MAGIC         ELSE TRY_CAST(grupo AS INT) 
# MAGIC     END AS numGrupo,
# MAGIC
# MAGIC     vertical,
# MAGIC     codigo_vertical AS codVertical,
# MAGIC     especialidad,
# MAGIC     codigo_especialidad AS codEspecialidad,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN TRIM(creditos) = '' THEN 0
# MAGIC         ELSE TRY_CAST(creditos AS DOUBLE) 
# MAGIC     END AS numCreditos,
# MAGIC
# MAGIC     codigo_programa AS codPrograma,
# MAGIC     admisionsino AS admiteAdmision,
# MAGIC     tiponegocio AS tipoNegocio,
# MAGIC     acreditado,
# MAGIC     nombreweb AS nombreWeb,
# MAGIC     entidad_legal AS entidadLegal,
# MAGIC     codigo_entidad_legal AS codEntidadLegal,
# MAGIC     modalidad,
# MAGIC     codigo_modalidad AS codModalidad,
# MAGIC
# MAGIC     -- Fechas con manejo de valores vacíos
# MAGIC     CASE 
# MAGIC         WHEN fecha_inicio IN ('', '00/00/0000', '0000-00-00') THEN NULL 
# MAGIC         ELSE TRY_CAST(fecha_inicio AS DATE) 
# MAGIC     END AS fechaInicio,
# MAGIC     
# MAGIC     CASE 
# MAGIC         WHEN fecha_fin IN ('', '00/00/0000', '0000-00-00') THEN NULL 
# MAGIC         ELSE TRY_CAST(fecha_fin AS DATE) 
# MAGIC     END AS fechaFin,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN TRIM(meses_duracion) = '' THEN 0
# MAGIC         ELSE TRY_CAST(meses_duracion AS INT) 
# MAGIC     END AS mesesDuracion,
# MAGIC
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
# MAGIC
# MAGIC     current_timestamp() AS created_at,
# MAGIC     current_timestamp() AS updated_at
# MAGIC
# MAGIC FROM silver_lakehouse.classlifetitulaciones;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_producto_view limit 25

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_producto AS target
# MAGIC USING (
# MAGIC   SELECT 
# MAGIC     codProductoOrigen,
# MAGIC     codProductoCorto,
# MAGIC     codProducto,
# MAGIC     origenProducto,
# MAGIC     tipoProducto,
# MAGIC     area,
# MAGIC     nombreOficial,
# MAGIC     curso,
# MAGIC     TRY_CAST(NULLIF(numeroCurso, '') AS INT) AS numeroCurso,
# MAGIC     TRY_CAST(NULLIF(ciclo_id, '') AS INT) AS ciclo_id,
# MAGIC     TRY_CAST(NULLIF(numPlazas, '') AS INT) AS numPlazas,
# MAGIC     TRY_CAST(NULLIF(numGrupo, '') AS INT) AS numGrupo,
# MAGIC     vertical,
# MAGIC     codVertical,
# MAGIC     especialidad,
# MAGIC     codEspecialidad,
# MAGIC     TRY_CAST(NULLIF(numCreditos, '') AS DOUBLE) AS numCreditos,
# MAGIC     codPrograma,
# MAGIC     admiteAdmision,
# MAGIC     tipoNegocio,
# MAGIC     acreditado,
# MAGIC     nombreWeb,
# MAGIC     entidadLegal,
# MAGIC     codEntidadLegal,
# MAGIC     modalidad,
# MAGIC     codModalidad,
# MAGIC     TRY_CAST(NULLIF(fechaInicioCurso, '-') AS DATE) AS fechaInicioCurso,
# MAGIC     TRY_CAST(NULLIF(fechaFinCurso, '-') AS DATE) AS fechaFinCurso,
# MAGIC     TRY_CAST(NULLIF(fechaInicio, '-') AS DATE) AS fechaInicio,
# MAGIC     TRY_CAST(NULLIF(fechaFin, '-') AS DATE) AS fechaFin,
# MAGIC     TRY_CAST(NULLIF(mesesDuracion, '') AS INT) AS mesesDuracion,
# MAGIC     TRY_CAST(NULLIF(horasAcreditadas, '') AS INT) AS horasAcreditadas,
# MAGIC     TRY_CAST(NULLIF(horasPresenciales, '') AS INT) AS horasPresenciales,
# MAGIC     TRY_CAST(NULLIF(fechaInicioPago, '-') AS DATE) AS fechaInicioPago,
# MAGIC     TRY_CAST(NULLIF(fechaFinPago, '-') AS DATE) AS fechaFinPago,
# MAGIC     TRY_CAST(NULLIF(numCuotas, '') AS INT) AS numCuotas,
# MAGIC     TRY_CAST(NULLIF(importeCertificado, '') AS DOUBLE) AS importeCertificado,
# MAGIC     TRY_CAST(NULLIF(importeAmpliacion, '') AS DOUBLE) AS importeAmpliacion,
# MAGIC     TRY_CAST(NULLIF(importeDocencia, '') AS DOUBLE) AS importeDocencia,
# MAGIC     TRY_CAST(NULLIF(importeMatricula, '') AS DOUBLE) AS importeMatricula,
# MAGIC     TRY_CAST(NULLIF(importeTotal, '') AS DOUBLE) AS importeTotal,
# MAGIC     TRY_CAST(NULLIF(ETLcreatedDate, '-') AS TIMESTAMP) AS ETLcreatedDate,
# MAGIC     TRY_CAST(NULLIF(ETLupdatedDate, '-') AS TIMESTAMP) AS ETLupdatedDate
# MAGIC   FROM dim_producto_view
# MAGIC   WHERE 
# MAGIC     codProductoOrigen IS NOT NULL AND 
# MAGIC     codProducto IS NOT NULL -- Evitar datos inconsistentes
# MAGIC ) AS source
# MAGIC ON target.codProductoOrigen = source.codProductoOrigen
# MAGIC AND target.codProducto = source.codProducto
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     target.codProductoCorto <> source.codProductoCorto OR
# MAGIC     target.origenProducto <> source.origenProducto OR
# MAGIC     target.tipoProducto <> source.tipoProducto OR
# MAGIC     target.area <> source.area OR
# MAGIC     target.nombreOficial <> source.nombreOficial OR
# MAGIC     target.curso <> source.curso OR
# MAGIC     target.numeroCurso <> source.numeroCurso OR
# MAGIC     target.ciclo_id <> source.ciclo_id OR
# MAGIC     target.numPlazas <> source.numPlazas OR
# MAGIC     target.numGrupo <> source.numGrupo OR
# MAGIC     target.fechaInicioCurso <> source.fechaInicioCurso OR
# MAGIC     target.fechaFinCurso <> source.fechaFinCurso OR
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
# MAGIC     target.codProductoCorto = COALESCE(source.codProductoCorto, target.codProductoCorto),
# MAGIC     target.origenProducto = COALESCE(source.origenProducto, target.origenProducto),
# MAGIC     target.tipoProducto = COALESCE(source.tipoProducto, target.tipoProducto),
# MAGIC     target.area = COALESCE(source.area, target.area),
# MAGIC     target.nombreOficial = COALESCE(source.nombreOficial, target.nombreOficial),
# MAGIC     target.curso = COALESCE(source.curso, target.curso),
# MAGIC     target.numeroCurso = COALESCE(source.numeroCurso, target.numeroCurso),
# MAGIC     target.ciclo_id = COALESCE(source.ciclo_id, target.ciclo_id),
# MAGIC     target.numPlazas = COALESCE(source.numPlazas, target.numPlazas),
# MAGIC     target.numGrupo = COALESCE(source.numGrupo, target.numGrupo),
# MAGIC     target.fechaInicioCurso = COALESCE(source.fechaInicioCurso, target.fechaInicioCurso),
# MAGIC     target.fechaFinCurso = COALESCE(source.fechaFinCurso, target.fechaFinCurso),
# MAGIC     target.fechaInicio = COALESCE(source.fechaInicio, target.fechaInicio),
# MAGIC     target.fechaFin = COALESCE(source.fechaFin, target.fechaFin),
# MAGIC     target.mesesDuracion = COALESCE(source.mesesDuracion, target.mesesDuracion),
# MAGIC     target.horasAcreditadas = COALESCE(source.horasAcreditadas, target.horasAcreditadas),
# MAGIC     target.horasPresenciales = COALESCE(source.horasPresenciales, target.horasPresenciales),
# MAGIC     target.fechaInicioPago = COALESCE(source.fechaInicioPago, target.fechaInicioPago),
# MAGIC     target.fechaFinPago = COALESCE(source.fechaFinPago, target.fechaFinPago),
# MAGIC     target.numCuotas = COALESCE(source.numCuotas, target.numCuotas),
# MAGIC     target.importeCertificado = COALESCE(source.importeCertificado, target.importeCertificado),
# MAGIC     target.importeAmpliacion = COALESCE(source.importeAmpliacion, target.importeAmpliacion),
# MAGIC     target.importeDocencia = COALESCE(source.importeDocencia, target.importeDocencia),
# MAGIC     target.importeMatricula = COALESCE(source.importeMatricula, target.importeMatricula),
# MAGIC     target.importeTotal = COALESCE(source.importeTotal, target.importeTotal),
# MAGIC     target.ETLupdatedDate = current_timestamp() -- Se actualiza solo si cambia el registro
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     codProductoOrigen, codProductoCorto, codProducto, origenProducto, tipoProducto, area, nombreOficial, curso, numeroCurso, ciclo_id, 
# MAGIC     numPlazas, numGrupo, vertical, codVertical, especialidad, codEspecialidad, numCreditos, codPrograma, admiteAdmision, tipoNegocio, 
# MAGIC     acreditado, nombreWeb, entidadLegal, codEntidadLegal, modalidad, codModalidad, fechaInicioCurso, fechaFinCurso, fechaInicio, fechaFin, 
# MAGIC     mesesDuracion, horasAcreditadas, horasPresenciales, fechaInicioPago, fechaFinPago, numCuotas, importeCertificado, importeAmpliacion, 
# MAGIC     importeDocencia, importeMatricula, importeTotal, ETLcreatedDate, ETLupdatedDate
# MAGIC   ) 
# MAGIC   VALUES (
# MAGIC     source.codProductoOrigen, source.codProductoCorto, source.codProducto, source.origenProducto, source.tipoProducto, source.area, 
# MAGIC     source.nombreOficial, source.curso, source.numeroCurso, source.ciclo_id, source.numPlazas, source.numGrupo, source.vertical, 
# MAGIC     source.codVertical, source.especialidad, source.codEspecialidad, source.numCreditos, source.codPrograma, source.admiteAdmision, 
# MAGIC     source.tipoNegocio, source.acreditado, source.nombreWeb, source.entidadLegal, source.codEntidadLegal, source.modalidad, source.codModalidad, 
# MAGIC     source.fechaInicioCurso, source.fechaFinCurso, source.fechaInicio, source.fechaFin, source.mesesDuracion, source.horasAcreditadas, 
# MAGIC     source.horasPresenciales, source.fechaInicioPago, source.fechaFinPago, source.numCuotas, source.importeCertificado, source.importeAmpliacion, 
# MAGIC     source.importeDocencia, source.importeMatricula, source.importeTotal, source.ETLcreatedDate, source.ETLupdatedDate
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_lakehouse.dim_producto LIMIT 100;

# COMMAND ----------

# DBTITLE 1,Validate duplicates >1
# MAGIC %sql
# MAGIC SELECT codProducto, COUNT(*) AS total_duplicados
# MAGIC FROM gold_lakehouse.dim_producto
# MAGIC GROUP BY codProducto
# MAGIC HAVING COUNT(*) > 1;
