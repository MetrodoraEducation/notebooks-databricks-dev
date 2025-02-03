# Databricks notebook source
# DBTITLE 1,ulac
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

endpoint_process_name = "enroll_groups"
table_name = "JsaClassLifeProductos"

classlifetitulaciones_df = spark.read.json(f"{bronze_folder_path}/lakehouse/classlife/{endpoint_process_name}/{current_date}/{table_name}.json")
classlifetitulaciones_df

# 📌 Inspeccionar el esquema inicial
print("📌 Esquema inicial antes de limpieza:")
classlifetitulaciones_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Pasos principales:**
# MAGIC - Limpiar el esquema completo del DataFrame antes de trabajar con las columnas anidadas.
# MAGIC - Desanidar las estructuras una por una asegurando que no existan conflictos.
# MAGIC - Revisar si existen estructuras complejas adicionales que deban manejarse de forma especial.

# COMMAND ----------

# 📌 Inspeccionar Esquema Inicial
print("📌 Esquema inicial antes de limpieza:")
classlifetitulaciones_df.printSchema()

# COMMAND ----------

# 📌 Función para limpiar nombres de columnas
def clean_column_names(df):
    """
    Limpia los nombres de columnas eliminando espacios, tildes y caracteres especiales.
    """
    for old_col in df.columns:
        new_col = (
            old_col.lower()
            .replace(" ", "_")
            .replace(".", "_")
            .replace("ñ", "n")
            .replace("ó", "o")
            .replace("á", "a")
            .replace("é", "e")
            .replace("í", "i")
            .replace("ú", "u")
        )
        df = df.withColumnRenamed(old_col, new_col)
    
    return df

# COMMAND ----------

# 📌 Extraer el contenido de `data` si existe
if "data" in classlifetitulaciones_df.columns:
    classlifetitulaciones_df = classlifetitulaciones_df.selectExpr("data.*")

# 📌 Inspeccionar después de extraer `data`
print("📌 Esquema después de seleccionar `data.*`:")
classlifetitulaciones_df.printSchema()

# COMMAND ----------

# 📌 Explotar `items` si es un array
if "items" in classlifetitulaciones_df.columns:
    print("📌 'items' es una estructura o array. Procedemos a desanidar.")

    # Si `items` es un array de estructuras, lo explotamos
    if isinstance(classlifetitulaciones_df.schema["items"].dataType, ArrayType):
        classlifetitulaciones_df = classlifetitulaciones_df.withColumn("items", explode(col("items")))

# COMMAND ----------

# 📌 Verificar esquema después de explotar `items`
print("📌 Esquema después de explotar `items`:")
classlifetitulaciones_df.printSchema()

# COMMAND ----------

# 📌 Extraer subcolumnas de `items`
if "items" in classlifetitulaciones_df.columns:
    subcolumns = classlifetitulaciones_df.select("items.*").columns  # Obtener nombres originales
    
    # 📌 Limpieza de nombres de columnas
    clean_subcolumns = [
        f"items.`{col_name}`" if " " in col_name or "." in col_name else f"items.{col_name}"
        for col_name in subcolumns
    ]

    # 📌 Extraer columnas de `items` y renombrarlas
    classlifetitulaciones_df = classlifetitulaciones_df.select(*[col(c).alias(c.replace("items.", "")) for c in clean_subcolumns])

# COMMAND ----------

# 📌 Inspeccionar después de desanidar `items`
print("📌 Esquema después de desanidar `items`:")
classlifetitulaciones_df.printSchema()


# 📌 Aplicar limpieza de nombres de columnas
classlifetitulaciones_df = clean_column_names(classlifetitulaciones_df)

# COMMAND ----------

# 📌 Inspeccionar después de limpiar nombres de columnas
print("📌 Esquema después de limpiar nombres de columnas:")
classlifetitulaciones_df.printSchema()

# COMMAND ----------

# 📌 Desanidar estructuras internas (`counters`, `metas`) si existen
if "counters" in classlifetitulaciones_df.columns:
    counters_cols = classlifetitulaciones_df.select("counters.*").columns
    classlifetitulaciones_df = classlifetitulaciones_df.select("*", *[col(f"counters.{c}").alias(f"counters_{c}") for c in counters_cols]).drop("counters")

if "metas" in classlifetitulaciones_df.columns:
    metas_cols = classlifetitulaciones_df.select("metas.*").columns
    classlifetitulaciones_df = classlifetitulaciones_df.select("*", *[col(f"metas.{c}").alias(f"metas_{c}") for c in metas_cols]).drop("metas")

# COMMAND ----------

# 📌 Inspeccionar después de expandir estructuras internas
print("📌 Esquema final después de desanidar estructuras:")
classlifetitulaciones_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, to_date, to_timestamp, lit, current_timestamp

def clean_column_names(df):
    """
    Limpia los nombres de columnas eliminando espacios, tildes, caracteres especiales
    y asegurando un formato estándar.
    """
    for old_col in df.columns:
        new_col = (
            old_col.lower()
            .replace(" ", "_")
            .replace(".", "_")
            .replace("ñ", "n")
            .replace("ó", "o")
            .replace("á", "a")
            .replace("é", "e")
            .replace("í", "i")
            .replace("ú", "u")
        )
        df = df.withColumnRenamed(old_col, new_col)
    
    return df

# 📌 Aplicar limpieza de nombres de columnas
classlifetitulaciones_df = clean_column_names(classlifetitulaciones_df)

# 📌 Renombrar columnas eliminando los prefijos "metas_" y "counters_"
renamed_columns = {}
for col_name in classlifetitulaciones_df.columns:
    new_col_name = col_name.replace("metas_", "").replace("counters_", "")
    if new_col_name not in renamed_columns.values():
        renamed_columns[col_name] = new_col_name

# 📌 Aplicar el renombramiento solo si el nombre no está repetido
for old_col, new_col in renamed_columns.items():
    classlifetitulaciones_df = classlifetitulaciones_df.withColumnRenamed(old_col, new_col)

# 📌 Lista de columnas a convertir en fecha (formato 'dd/MM/yyyy')
date_columns = [
    "fecha_inicio_docencia", "fecha_fin_cuotas", "fecha_fin_reconocimiento_ingresos",
    "fecha_inicio_reconocimiento_ingresos", "fecha_fin_docencia", "fecha_inicio_cuotas"
]

for col_name in date_columns:
    classlifetitulaciones_df = classlifetitulaciones_df.withColumn(col_name, to_date(col(col_name), "dd/MM/yyyy"))

# 📌 Verificar los tipos de las columnas antes de convertir
print("Schema antes de conversión:")
classlifetitulaciones_df.printSchema()

# 📌 Convertir `fecha_creacion` y `ultima_actualizacion` a TIMESTAMP asegurando el formato correcto
datetime_columns = ["fecha_creacion", "ultima_actualizacion"]

for col_name in datetime_columns:
    classlifetitulaciones_df = classlifetitulaciones_df.withColumn(col_name, 
        to_timestamp(col(col_name), "yyyy-MM-dd HH:mm:ss")
    )

# 📌 Agregar columnas `processdate` y `sourceSystem`
classlifetitulaciones_df = classlifetitulaciones_df.withColumn("processdate", current_timestamp())
classlifetitulaciones_df = classlifetitulaciones_df.withColumn("sourceSystem", lit("classlifetitulaciones"))

# 📌 Lista de columnas finales después de limpiar prefijos y evitar duplicados
columnas_finales = [
    "ano_inicio_docencia", "certificado_euneiz_incluido", "cuotas_docencia", "entidad_legal", 
    "entidad_legal_codigo", "fecha_fin_pago", "fecha_inicio_pago", "fecha_fin", "fecha_inicio", 
    "grupo", "horas_acreditadas", "horas_presenciales", "mes_inicio_docencia", "meses_duracion", 
    "modalidad", "no_ultimas_plazas", "sede", "tarifa_ampliacion", "tarifa_docencia", "tarifa_euneiz", 
    "tarifa_matricula", "total_tarifas", "vertical", "acreditado", "admisionsino", "area_id", 
    "area_title", "building_id", "building_title", "ciclo_id", "ciclo_title", "codigo_antiguo", 
    "codigo_especialidad", "codigo_programa", "codigo_vertical", "creditos", "degree_id", 
    "degree_title", "descripcion_calendario", "destinatarios", "enroll_alias", "enroll_end", 
    "enroll_group_id", "enroll_group_name", "enroll_ini", "especialidad", "fecha_creacion", 
    "modalidad_code", "nombre_antiguo_de_programa", "nombre_del_programa_oficial_completo", 
    "nombreweb", "plan_id", "plan_title", "plazas", "school_id", "school_name", "section_id", 
    "section_title", "term_id", "term_title", "tiponegocio", "ultima_actualizacion", "year", 
    "availables", "enroll_group_id", "enrolled", "pre_enrolled", "seats", "admisionsino", 
    "ano_inicio_docencia", "building", "certificado_euneiz_incluido", "codigo_entidad_legal", 
    "codigo_modalidad", "codigo_sede", "codigo_vertical", "descripcion_calendario", "enroll_pago_ini_t", 
    "excludeSecurityArrayMetas", "fecha_fin_cuotas", "fecha_fin_docencia", 
    "fecha_fin_reconocimiento_ingresos", "fecha_inicio_cuotas", "fecha_inicio_docencia", 
    "fecha_inicio_reconocimiento_ingresos", "grupo", "grupos_cerrados", "horas_acreditadas", 
    "horas_presenciales", "mes_inicio_docencia", "mesesAmpliacion", "meses_cursos_open", 
    "num_alumnos_inscritos", "num_plazas", "num_plazas_ultimas", "receipts_count", "roaster_ind", 
    "tiponegocio", "processdate", "sourceSystem"
]

# 📌 Eliminar columnas duplicadas en la lista de selección
columnas_finales = list(set(columnas_finales))  # Convierte a set y de nuevo a lista para eliminar duplicados

# 📌 Filtrar solo las columnas que existen en el DataFrame
columnas_disponibles = [col for col in columnas_finales if col in classlifetitulaciones_df.columns]

# 📌 Si no hay coincidencias, mostramos advertencia
if not columnas_disponibles:
    print("⚠️ Ninguna de las columnas esperadas está disponible en el DataFrame. Verifica la limpieza de nombres.")

# 📌 Seleccionar solo las columnas disponibles
classlifetitulaciones_df = classlifetitulaciones_df.select(*columnas_disponibles)

# 📌 Mostrar los primeros registros
display(classlifetitulaciones_df)

# COMMAND ----------

classlifetitulaciones_df = classlifetitulaciones_df.dropDuplicates()

# COMMAND ----------

classlifetitulaciones_df.createOrReplaceTempView("classlifetitulaciones_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.classlifetitulaciones AS target
# MAGIC USING classlifetitulaciones_view AS source
# MAGIC ON target.enroll_group_id = source.enroll_group_id
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.school_name = source.school_name,
# MAGIC         target.degree_title = source.degree_title,
# MAGIC         target.area_title = source.area_title,
# MAGIC         target.year = source.year,
# MAGIC         target.vertical = source.vertical
# MAGIC         -- Agregar todas las columnas necesarias
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (enroll_group_id, school_name, degree_title, area_title, year, vertical) 
# MAGIC     VALUES (source.enroll_group_id, source.school_name, source.degree_title, source.area_title, source.year, source.vertical);
