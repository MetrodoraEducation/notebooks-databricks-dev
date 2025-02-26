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
# MAGIC         WHEN fecha_inicio IN ('', '00/00/0000', '0000-00-00') THEN NULL 
# MAGIC         ELSE TRY_CAST(fecha_inicio AS DATE) 
# MAGIC     END AS fechaInicioCurso,
# MAGIC     CASE 
# MAGIC         WHEN fecha_fin IN ('', '00/00/0000', '0000-00-00') THEN NULL 
# MAGIC         ELSE TRY_CAST(fecha_fin AS DATE) 
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
# MAGIC     UPPER(codigo_programa) AS codPrograma,
# MAGIC     admisionsino AS admiteAdmision,
# MAGIC     tiponegocio AS tipoNegocio,
# MAGIC     acreditado,
# MAGIC     nombreweb AS nombreWeb,
# MAGIC     entidad_legal AS entidadLegal,
# MAGIC     codigo_entidad_legal AS codEntidadLegal,
# MAGIC     modalidad,
# MAGIC     modalidad_code AS codModalidad,
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
# MAGIC -- 1️⃣ Asegurar que el registro `-1` siempre existe con valores `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_producto AS target
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         '-1' AS codProductoOrigen, 'n/a' AS codProductoCorto, 'n/a' AS codProducto, 'n/a' AS origenProducto,
# MAGIC         'n/a' AS tipoProducto, 'n/a' AS area, 'n/a' AS nombreOficial, 'n/a' AS curso, 0 AS numeroCurso,
# MAGIC         '1900-01-01' AS fechaInicioCurso, '1900-01-01' AS fechaFinCurso, 0 AS ciclo_id, 0 AS numPlazas,
# MAGIC         0 AS numGrupo, 'n/a' AS vertical, 'n/a' AS codVertical, 'n/a' AS especialidad, 'n/a' AS codEspecialidad,
# MAGIC         0.0 AS numCreditos, 'n/a' AS codPrograma, 'n/a' AS admiteAdmision, 'n/a' AS tipoNegocio, 'n/a' AS acreditado,
# MAGIC         'n/a' AS nombreWeb, 'n/a' AS entidadLegal, 'n/a' AS codEntidadLegal, 'n/a' AS modalidad, 'n/a' AS codModalidad,
# MAGIC         '1900-01-01' AS fechaInicio, '1900-01-01' AS fechaFin, 0 AS mesesDuracion, 0 AS horasAcreditadas, 0 AS horasPresenciales,
# MAGIC         '1900-01-01' AS fechaInicioPago, '1900-01-01' AS fechaFinPago, 0 AS numCuotas, 0.0 AS importeCertificado,
# MAGIC         0.0 AS importeAmpliacion, 0.0 AS importeDocencia, 0.0 AS importeMatricula, 0.0 AS importeTotal,
# MAGIC         CURRENT_TIMESTAMP AS ETLupdatedDate, CURRENT_TIMESTAMP AS ETLcreatedDate
# MAGIC ) AS source
# MAGIC ON target.idDimProducto = -1
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (
# MAGIC         codProductoOrigen, codProductoCorto, codProducto, origenProducto, tipoProducto, area, nombreOficial, curso, numeroCurso, 
# MAGIC         fechaInicioCurso, fechaFinCurso, ciclo_id, numPlazas, numGrupo, vertical, codVertical, especialidad, codEspecialidad, numCreditos, 
# MAGIC         codPrograma, admiteAdmision, tipoNegocio, acreditado, nombreWeb, entidadLegal, codEntidadLegal, modalidad, codModalidad, fechaInicio, 
# MAGIC         fechaFin, mesesDuracion, horasAcreditadas, horasPresenciales, fechaInicioPago, fechaFinPago, numCuotas, importeCertificado, 
# MAGIC         importeAmpliacion, importeDocencia, importeMatricula, importeTotal, ETLcreatedDate, ETLupdatedDate
# MAGIC     ) VALUES (
# MAGIC         source.codProductoOrigen, source.codProductoCorto, source.codProducto, source.origenProducto, source.tipoProducto, source.area, 
# MAGIC         source.nombreOficial, source.curso, source.numeroCurso, source.fechaInicioCurso, source.fechaFinCurso, source.ciclo_id, source.numPlazas, 
# MAGIC         source.numGrupo, source.vertical, source.codVertical, source.especialidad, source.codEspecialidad, source.numCreditos, source.codPrograma, 
# MAGIC         source.admiteAdmision, source.tipoNegocio, source.acreditado, source.nombreWeb, source.entidadLegal, source.codEntidadLegal, source.modalidad, 
# MAGIC         source.codModalidad, source.fechaInicio, source.fechaFin, source.mesesDuracion, source.horasAcreditadas, source.horasPresenciales, 
# MAGIC         source.fechaInicioPago, source.fechaFinPago, source.numCuotas, source.importeCertificado, source.importeAmpliacion, source.importeDocencia, 
# MAGIC         source.importeMatricula, source.importeTotal, source.ETLcreatedDate, source.ETLupdatedDate
# MAGIC     );
# MAGIC
# MAGIC -- 2️⃣ MERGE principal para `dim_producto`, evitando modificar el registro `-1`
# MAGIC MERGE INTO gold_lakehouse.dim_producto AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT * FROM dim_producto_view 
# MAGIC     WHERE codProductoOrigen <> '-1'
# MAGIC ) AS source
# MAGIC ON UPPER(TRIM(target.codProductoOrigen)) = UPPER(TRIM(source.codProductoOrigen)) 
# MAGIC    AND UPPER(TRIM(target.codProducto)) = UPPER(TRIM(source.codProducto))
# MAGIC    AND target.idDimProducto != -1  -- 🔹 Evitar modificar el registro `-1`
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     COALESCE(TRIM(UPPER(target.nombreOficial)), '') <> COALESCE(TRIM(UPPER(source.nombreOficial)), '') OR
# MAGIC     COALESCE(TRIM(UPPER(target.tipoProducto)), '') <> COALESCE(TRIM(UPPER(source.tipoProducto)), '') OR
# MAGIC     COALESCE(TRIM(UPPER(target.area)), '') <> COALESCE(TRIM(UPPER(source.area)), '') OR
# MAGIC     COALESCE(TRIM(UPPER(target.curso)), '') <> COALESCE(TRIM(UPPER(source.curso)), '') OR
# MAGIC     COALESCE(TRIM(UPPER(target.vertical)), '') <> COALESCE(TRIM(UPPER(source.vertical)), '') OR
# MAGIC     COALESCE(TRIM(UPPER(target.codVertical)), '') <> COALESCE(TRIM(UPPER(source.codVertical)), '') OR
# MAGIC     COALESCE(TRIM(UPPER(target.modalidad)), '') <> COALESCE(TRIM(UPPER(source.modalidad)), '') OR
# MAGIC     COALESCE(TRIM(UPPER(target.codModalidad)), '') <> COALESCE(TRIM(UPPER(source.codModalidad)), '') OR
# MAGIC     COALESCE(TRIM(UPPER(target.nombreWeb)), '') <> COALESCE(TRIM(UPPER(source.nombreWeb)), '') OR
# MAGIC     target.ETLupdatedDate < source.ETLupdatedDate
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     target.nombreOficial = source.nombreOficial,
# MAGIC     target.tipoProducto = source.tipoProducto,
# MAGIC     target.area = source.area,
# MAGIC     target.curso = source.curso,
# MAGIC     target.vertical = source.vertical,
# MAGIC     target.codVertical = source.codVertical,
# MAGIC     target.modalidad = source.modalidad,
# MAGIC     target.codModalidad = source.codModalidad,
# MAGIC     target.nombreWeb = source.nombreWeb,
# MAGIC     target.ETLupdatedDate = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         codProductoOrigen, codProductoCorto, codProducto, origenProducto, tipoProducto, area, nombreOficial, curso, numeroCurso, 
# MAGIC         fechaInicioCurso, fechaFinCurso, ciclo_id, numPlazas, numGrupo, vertical, codVertical, especialidad, codEspecialidad, numCreditos, 
# MAGIC         codPrograma, admiteAdmision, tipoNegocio, acreditado, nombreWeb, entidadLegal, codEntidadLegal, modalidad, codModalidad, fechaInicio, 
# MAGIC         fechaFin, mesesDuracion, horasAcreditadas, horasPresenciales, fechaInicioPago, fechaFinPago, numCuotas, importeCertificado, 
# MAGIC         importeAmpliacion, importeDocencia, importeMatricula, importeTotal, ETLcreatedDate, ETLupdatedDate
# MAGIC     ) VALUES (
# MAGIC         source.codProductoOrigen, source.codProductoCorto, source.codProducto, source.origenProducto, source.tipoProducto, source.area, 
# MAGIC         source.nombreOficial, source.curso, source.numeroCurso, source.fechaInicioCurso, source.fechaFinCurso, source.ciclo_id, source.numPlazas, 
# MAGIC         source.numGrupo, source.vertical, source.codVertical, source.especialidad, source.codEspecialidad, source.numCreditos, source.codPrograma, 
# MAGIC         source.admiteAdmision, source.tipoNegocio, source.acreditado, source.nombreWeb, source.entidadLegal, source.codEntidadLegal, source.modalidad, 
# MAGIC         source.codModalidad, source.fechaInicio, source.fechaFin, source.mesesDuracion, source.horasAcreditadas, source.horasPresenciales, 
# MAGIC         source.fechaInicioPago, source.fechaFinPago, source.numCuotas, source.importeCertificado, source.importeAmpliacion, source.importeDocencia, 
# MAGIC         source.importeMatricula, source.importeTotal, source.ETLcreatedDate, source.ETLupdatedDate
# MAGIC     );
# MAGIC

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
