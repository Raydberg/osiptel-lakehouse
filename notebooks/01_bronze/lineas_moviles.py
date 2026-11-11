from pyspark.sql import functions as F

source_path = (
    "/Volumes/osiptel_lakehouse/landing/osiptel_raw/"
    "lineas_moviles/1.1 LÍNEAS MÓVILES.xlsx"
)

target_table = "osiptel_lakehouse.bronze.lineas_moviles"


# ---------------------------------------------------------
# 1. Lectura del archivo original
# ---------------------------------------------------------

df_raw = (
    spark.read
    .format("excel")
    .option("headerRows", 1)
    .option("dataAddress", "Dataset!B4:F1048576")
    .load(source_path)
)


# ---------------------------------------------------------
# 2. Eliminar filas posteriores completamente vacías
# ---------------------------------------------------------

df_raw = df_raw.filter(
    F.col(df_raw.columns[0]).isNotNull()
)


# ---------------------------------------------------------
# 3. Validación de estructura
# ---------------------------------------------------------

expected_column_count = 5

if len(df_raw.columns) != expected_column_count:
    raise ValueError(
        f"Se esperaban {expected_column_count} columnas, "
        f"pero se encontraron {len(df_raw.columns)}: "
        f"{df_raw.columns}"
    )

print("Columnas originales:")

for column in df_raw.columns:
    print(f"- {column}")

print(f"\nCantidad de columnas: {len(df_raw.columns)}")


# ---------------------------------------------------------
# 4. Vista previa
# ---------------------------------------------------------

display(df_raw.limit(20))

source_count_raw = df_raw.count()

print(f"\nCantidad de registros encontrados: {source_count_raw}")

df_raw.printSchema()


# ---------------------------------------------------------
# 5. Estandarización técnica de nombres
# ---------------------------------------------------------

bronze_columns = [
    "empresa_operadora",
    "departamento",
    "modalidad_contractual",
    "mes",
    "cantidad_lineas"
]

df_bronze = df_raw.toDF(*bronze_columns)

display(df_bronze.limit(20))

# ---------------------------------------------------------
# 6. Metadatos de trazabilidad
# ---------------------------------------------------------

df_bronze = (
    df_bronze
    .withColumn("_source_file", F.lit(source_path))
    .withColumn("_source_dataset", F.lit("lineas_moviles"))
    .withColumn("_source_sheet", F.lit("Dataset"))
    .withColumn("_ingested_at", F.current_timestamp())
)
display(df_bronze.limit(20))
df_bronze.printSchema()

# ---------------------------------------------------------
# 7. Validación previa
# ---------------------------------------------------------

source_count = df_bronze.count()

print("=== VALIDACIÓN PREVIA ===")
print(f"Registros a cargar: {source_count}")

df_bronze.printSchema()
# ---------------------------------------------------------
# 8. Persistencia Delta
# ---------------------------------------------------------

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Tabla creada correctamente: {target_table}")
# ---------------------------------------------------------
# 9. Validación posterior
# ---------------------------------------------------------

table_count = spark.table(target_table).count()

print("=== VALIDACIÓN POSTERIOR ===")
print(f"Registros antes de guardar: {source_count}")
print(f"Registros almacenados:      {table_count}")

assert source_count == table_count, (
    "El número de registros cambió durante la carga."
)

display(
    spark.sql("""
        SELECT *
        FROM osiptel_lakehouse.bronze.lineas_moviles
        LIMIT 20
    """)
)
