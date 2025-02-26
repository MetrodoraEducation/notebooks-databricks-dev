# Databricks notebook source
# DBTITLE 1,ulac
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

endpoint_process_name = "enroll_groups"
table_name = "JsaClassLifeProductos"

classlifetitulaciones_df = spark.read.json(f"{bronze_folder_path}/lakehouse/classlife/{endpoint_process_name}/{current_date}/{table_name}.json")
classlifetitulaciones_df

# COMMAND ----------

# MAGIC %md
# MAGIC **Pasos principales:**
# MAGIC - Limpiar el esquema completo del DataFrame antes de trabajar con las columnas anidadas.
# MAGIC - Desanidar las estructuras una por una asegurando que no existan conflictos.
# MAGIC - Revisar si existen estructuras complejas adicionales que deban manejarse de forma especial.

# COMMAND ----------

# 📌 Listar columnas actuales antes de desanidar
print("📌 Columnas iniciales en el DataFrame:")
print(classlifetitulaciones_df.columns)

# COMMAND ----------

# DBTITLE 1,Desanida data
from pyspark.sql.functions import col, explode
from pyspark.sql.types import ArrayType, StructType

# 📌 Verificar si `data` existe en el DataFrame
if "data" in classlifetitulaciones_df.columns:
    print("\n✅ 'data' detectado. Procedemos a analizar su tipo.")

    # Obtener el tipo de `data`
    data_type = classlifetitulaciones_df.schema["data"].dataType

    # 📌 Si `data` es un ARRAY de estructuras, explotar antes de desanidar
    if isinstance(data_type, ArrayType) and isinstance(data_type.elementType, StructType):
        print("\n✅ 'data' es un ARRAY de estructuras. Procedemos a explotar.")
        classlifetitulaciones_df = classlifetitulaciones_df.withColumn("data", explode(col("data")))

    # 📌 Extraer subcolumnas de `data`
    print("\n✅ Extrayendo subcolumnas de 'data'")
    data_cols = classlifetitulaciones_df.select("data.*").columns

    # 📌 Crear nuevo DataFrame con `data.*` extraído
    classlifetitulaciones_df = classlifetitulaciones_df.select(
        "*",  # Mantiene todas las columnas originales
        *[col(f"data.{c}").alias(c) for c in data_cols]  # Extrae columnas internas de `data`
    ).drop("data")  # Elimina `data` después de extraer sus valores

    print("\n✅ 'data' ha sido explotado y desanidado con éxito.")

else:
    print("\n⚠️ 'data' NO encontrado en el DataFrame.")

# 📌 Inspeccionar resultado final
print("\n📌 Esquema después del desanidado de `data`:")
classlifetitulaciones_df.printSchema()

# 📌 Mostrar resultado final
display(classlifetitulaciones_df)

# COMMAND ----------

# 📌 Inspeccionar después del desanidado
print("\n📌 Esquema después del desanidado de `data`:")
classlifetitulaciones_df.printSchema()

# 📌 Mostrar resultado final
display(classlifetitulaciones_df)


# COMMAND ----------

from pyspark.sql.functions import col, explode
from pyspark.sql.types import ArrayType, StructType

# 📌 Verificar si `items` existe en el DataFrame
if "items" in classlifetitulaciones_df.columns:
    print("\n✅ 'items' detectado. Procedemos a analizar su tipo.")

    # Obtener tipo de datos de `items`
    items_type = classlifetitulaciones_df.schema["items"].dataType

    # 📌 Si `items` es un array de estructuras, lo explotamos
    if isinstance(items_type, ArrayType) and isinstance(items_type.elementType, StructType):
        print("\n✅ 'items' es un ARRAY de estructuras. Procedemos a explotar.")
        
        # Explotamos `items`
        classlifetitulaciones_df = classlifetitulaciones_df.withColumn("items", explode(col("items")))

        # 📌 Extraer columnas internas de `items`
        items_cols = classlifetitulaciones_df.select("items.*").columns

        # 📌 Renombrar las columnas y agregarlas al DataFrame
        classlifetitulaciones_df = classlifetitulaciones_df.select(
            "*",  # Mantiene todas las columnas originales
            *[col(f"items.{c}").alias(f"items_{c}") for c in items_cols]
        ).drop("items")  # Elimina `items` original después de extraer sus valores

        print("\n✅ 'items' ha sido explotado y desanidado con éxito.")
    
    else:
        print("\n⚠️ 'items' NO es un array de estructuras. No se realizará ninguna transformación.")
else:
    print("\n⚠️ 'items' NO encontrado en el DataFrame.")

display(classlifetitulaciones_df)

# COMMAND ----------

# DBTITLE 1,Desanida counters
from pyspark.sql.functions import col
from pyspark.sql.types import StructType

# 📌 Verificar si `counters` existe y es una estructura antes de desanidar
if "counters" in classlifetitulaciones_df.columns and isinstance(classlifetitulaciones_df.schema["counters"].dataType, StructType):
    print("📌 'counters' es una estructura. Procedemos a desanidar.")

    # 📌 Obtener subcolumnas de `counters`, excluyendo `enroll_group_id`
    counters_cols = [
        c for c in classlifetitulaciones_df.select("counters.*").columns if c != "enroll_group_id"
    ]

    if counters_cols:
        # 📌 Extraer y renombrar las columnas de `counters`
        classlifetitulaciones_df = classlifetitulaciones_df.select(
            "*",  # Mantiene todas las columnas originales
            *[col(f"counters.{c}").alias(f"counters_{c}") for c in counters_cols]  # Renombrar subcolumnas
        )

    # 📌 Eliminar `counters` original después de extraer sus datos
    classlifetitulaciones_df = classlifetitulaciones_df.drop("counters")

# 📌 Mostrar resultado final
display(classlifetitulaciones_df)


# COMMAND ----------

# 📌 Inspeccionar después de desanidar `items`
print("📌 Esquema después de desanidar `items` y 'counters':")
classlifetitulaciones_df.printSchema()


# 📌 Aplicar limpieza de nombres de columnas
classlifetitulaciones_df = clean_column_names(classlifetitulaciones_df)

display(classlifetitulaciones_df)

# COMMAND ----------

# 📌 Inspeccionar después de limpiar nombres de columnas
print("📌 Esquema después de limpiar nombres de columnas:")
classlifetitulaciones_df.printSchema()

display(classlifetitulaciones_df)

# COMMAND ----------

# 📌 Desanidar estructuras internas (counters, metas) si existen
if "counters" in classlifetitulaciones_df.columns:
    counters_cols = classlifetitulaciones_df.select("counters.*").columns
    classlifetitulaciones_df = classlifetitulaciones_df.select("*", *[col(f"counters.{c}").alias(f"counters_{c}") for c in counters_cols]).drop("counters")

if "metas" in classlifetitulaciones_df.columns:
    metas_cols = classlifetitulaciones_df.select("metas.*").columns
    classlifetitulaciones_df = classlifetitulaciones_df.select("*", *[col(f"metas.{c}").alias(f"metas_{c}") for c in metas_cols]).drop("metas")

display(classlifetitulaciones_df)

# COMMAND ----------

# 📌 Inspeccionar después de expandir estructuras internas
print("📌 Esquema final después de desanidar estructuras:")
classlifetitulaciones_df.printSchema()

display(classlifetitulaciones_df)

# COMMAND ----------

from pyspark.sql.functions import col

def clean_and_keep_one_enroll_group_id(df):
    """
    - Asegura que solo haya una columna `enroll_group_id` en el DataFrame.
    - Si existen `enroll_group_id_2`, `enroll_group_id_3`, etc., las elimina.
    - Mantiene solo la primera aparición de `enroll_group_id`.
    """
    
    # 📌 Detectar columnas duplicadas antes de realizar operaciones
    column_counts = {}
    for col_name in df.columns:
        column_counts[col_name] = column_counts.get(col_name, 0) + 1

    # 📌 Identificar columnas `enroll_group_id` duplicadas
    final_columns = []
    enroll_group_id_seen = False  # Controla si ya tomamos `enroll_group_id`

    for col_name in df.columns:
        if col_name == "enroll_group_id":
            if enroll_group_id_seen:
                print(f"⚠️ Eliminando columna duplicada: {col_name}")
                continue  # Evita agregar más de una vez `enroll_group_id`
            enroll_group_id_seen = True

        elif col_name.startswith("enroll_group_id_"):  # Si hay versiones duplicadas, eliminarlas
            print(f"⚠️ Eliminando columna extra: {col_name}")
            continue

        # Agregar columna solo si no es una duplicada de `enroll_group_id`
        final_columns.append(col_name)

    # 📌 Seleccionar solo las columnas que queremos conservar
    df = df.select(*[col(c) for c in final_columns])

    return df

# 🚀 **ANTES DE HACER CUALQUIER OTRA OPERACIÓN, eliminamos duplicados**
classlifetitulaciones_df = clean_and_keep_one_enroll_group_id(classlifetitulaciones_df)

# 📌 Verificar que solo queda una `enroll_group_id`
assert classlifetitulaciones_df.columns.count("enroll_group_id") == 1, "❌ Hay más de una `enroll_group_id`"

print("\n✅ Se ha eliminado cualquier duplicado de `enroll_group_id` correctamente.")

# 📌 Mostrar los primeros registros
display(classlifetitulaciones_df)


# COMMAND ----------

from pyspark.sql.functions import col, to_date, to_timestamp, lit, current_timestamp
from pyspark.sql.types import StringType, IntegerType, DoubleType

# 📌 Verificar columnas duplicadas
column_counts = {col_name: classlifetitulaciones_df.columns.count(col_name) for col_name in set(classlifetitulaciones_df.columns)}
duplicated_columns = [col_name for col_name, count in column_counts.items() if count > 1]

if duplicated_columns:
    print(f"⚠️ Columnas duplicadas detectadas: {duplicated_columns}")

    # Eliminar duplicados conservando solo la primera aparición
    selected_columns = []
    seen_columns = set()

    for col_name in classlifetitulaciones_df.columns:
        if col_name not in seen_columns:
            seen_columns.add(col_name)
            selected_columns.append(col(col_name))

    # Filtrar el DataFrame con solo las columnas únicas
    classlifetitulaciones_df = classlifetitulaciones_df.select(*selected_columns)



# COMMAND ----------

classlifetitulaciones_df.createOrReplaceTempView("classlifetitulaciones_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.classlifetitulaciones AS target
# MAGIC USING classlifetitulaciones_view AS source
# MAGIC ON target.enroll_group_id = source.enroll_group_id
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_lakehouse.classlifetitulaciones
