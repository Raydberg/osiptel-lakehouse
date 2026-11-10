from pyspark.sql import functions as F

source_path = (
    "/Volumes/osiptel_lakehouse/landing/osiptel_raw/"
    "reclamos_presentados/10.1 RECLAMOS PRESENTADOS.xlsx"
)

target_table = "osiptel_lakehouse.bronze.reclamos_presentados"


# ---------------------------------------------------------
# 1. Lectura del archivo original
# ---------------------------------------------------------

df_raw = (
    spark.read
    .format("excel")
    .option("headerRows", 1)
    .option("dataAddress", "Dataset!B4:H1048576")
    .load(source_path)
)


# ---------------------------------------------------------
# 2. Eliminar filas completamente ajenas al dataset
# ---------------------------------------------------------

df_raw = df_raw.filter(
    F.col(df_raw.columns[0]).isNotNull()
)


# ---------------------------------------------------------
# 3. Validación de estructura
# ---------------------------------------------------------

expected_column_count = 7

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
# 4. Normalización técnica de nombres
# ---------------------------------------------------------

bronze_columns = [
    "empresa_operadora",
    "mes",
    "departamento",
    "servicio_involucrado",
    "forma_presentacion",
    "materia_reclamable",
    "cantidad_reclamos_presentados"
]

df_bronze = df_raw.toDF(*bronze_columns)


# ---------------------------------------------------------
# 5. Metadatos de trazabilidad
# ---------------------------------------------------------

df_bronze = (
    df_bronze
    .withColumn("_source_file", F.lit(source_path))
    .withColumn("_source_dataset", F.lit("reclamos_presentados"))
    .withColumn("_ingested_at", F.current_timestamp())
)


# Evita volver a leer el XLSX en cada acción durante las validaciones
df_bronze.cache()


# ---------------------------------------------------------
# 6. Validación previa a la carga
# ---------------------------------------------------------

print(f"Registros a cargar: {df_bronze.count()}")

df_bronze.printSchema()

display(df_bronze.limit(20))

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Tabla creada correctamente: {target_table}")


display(
    spark.sql("""
        SELECT *
        FROM osiptel_lakehouse.bronze.reclamos_presentados
        LIMIT 20
    """)
)