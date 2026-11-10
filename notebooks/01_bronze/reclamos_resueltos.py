from pyspark.sql import functions as F

source_path = (
    "/Volumes/osiptel_lakehouse/landing/osiptel_raw/"
    "reclamos_resueltos/10.2 RECLAMOS RESUELTOS.xlsx"
)

target_table = "osiptel_lakehouse.bronze.reclamos_resueltos"

sheets = (
    spark.read
    .format("excel")
    .option("operation", "listSheets")
    .load(source_path)
)

# ------------------------------------------------------
# Lectura real de la tabla
# ------------------------------------------------------

df_raw = (
    spark.read
    .format("excel")
    .option("headerRows", 1)
    .option("dataAddress", "Dataset!B4:I1048576")
    .load(source_path)
)

# Eliminar únicamente filas vacías posteriores al contenido
df_raw = df_raw.filter(
    F.col(df_raw.columns[0]).isNotNull()
)

display(df_raw.limit(20))

print("Columnas encontradas:")

for column in df_raw.columns:
    print(f"- {column}")

print(f"\nCantidad de columnas: {len(df_raw.columns)}")
print(f"Cantidad de registros: {df_raw.count()}")

df_raw.printSchema()

bronze_columns = [
    "empresa_operadora",
    "mes",
    "departamento",
    "servicio_involucrado",
    "medio_presentacion",
    "materia_reclamable",
    "sentido_resolucion",
    "cantidad_reclamos_resueltos"
]

if len(df_raw.columns) != len(bronze_columns):
    raise ValueError(
        f"Se esperaban {len(bronze_columns)} columnas, "
        f"pero se encontraron {len(df_raw.columns)}"
    )

df_bronze = df_raw.toDF(*bronze_columns)

display(df_bronze.limit(20))

df_bronze = (
    df_bronze
    .withColumn("_source_file", F.lit(source_path))
    .withColumn("_source_dataset", F.lit("reclamos_resueltos"))
    .withColumn("_source_sheet", F.lit("Dataset"))
    .withColumn("_ingested_at", F.current_timestamp())
)

display(df_bronze.limit(20))
df_bronze.printSchema()


source_count = df_bronze.count()


print(f"Registros a cargar: {source_count}")



(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Tabla creada correctamente: {target_table}")





table_count = spark.table(target_table).count()

print(f"Registros antes de guardar: {source_count}")
print(f"Registros almacenados:      {table_count}")

assert source_count == table_count, (
    "El número de registros cambió durante la carga."
)

display(
    spark.sql("""
        SELECT *
        FROM osiptel_lakehouse.bronze.reclamos_resueltos
        LIMIT 20
    """)
)

print("=== VALIDACIÓN BRONZE: RECLAMOS RESUELTOS ===")

print("Registros:")
print(spark.table(target_table).count())

print("Esquema:")
spark.table(target_table).printSchema()

print("Medios de presentación distintos:")
display(
    spark.table(target_table)
    .select("medio_presentacion")
    .distinct()
    .orderBy("medio_presentacion")
)

print("Sentidos de resolución distintos:")
display(
    spark.table(target_table)
    .select("sentido_resolucion")
    .distinct()
    .orderBy("sentido_resolucion")
)
