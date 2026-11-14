from pyspark.sql import functions as F

from shared.cleaning import (
    clean_text,
    normalize_departamento
)

# ============================================================
# 1. CONFIGURACIÓN
# ============================================================

source_table = "osiptel_lakehouse.bronze.reclamos_presentados"
target_table = "osiptel_lakehouse.silver.reclamos_presentados"
reject_table = "osiptel_lakehouse.silver.reclamos_presentados_rechazados"

spark.sql("CREATE SCHEMA IF NOT EXISTS osiptel_lakehouse.silver")


# ============================================================
# 2. LECTURA DE BRONZE
# ============================================================

df_bronze = spark.table(source_table)
# Compatibilidad con tablas Bronze creadas antes
# de incorporar el metadato _source_sheet
if "_source_sheet" not in df_bronze.columns:"departamento",$0
    df_bronze = df_bronze.withColumn(
        "_source_sheet",
        F.lit("Dataset")
    )
bronze_count = df_bronze.count()

print("==============================================")
print("SILVER - RECLAMOS PRESENTADOS")
print("==============================================")
print(f"Registros recibidos desde Bronze: {bronze_count}")

display(df_bronze.limit(20))

df_bronze.printSchema()

# ============================================================
# 4. LIMPIEZA Y ESTANDARIZACIÓN
# ============================================================

df_clean = (
    df_bronze

    # --------------------------------------------------------
    # Conservar valor original del operador
    # --------------------------------------------------------
    .withColumn(
        "empresa_operadora_original",
        F.col("empresa_operadora")
    )

    # --------------------------------------------------------
    # Normalización textual
    # --------------------------------------------------------
    .withColumn(
        "empresa_operadora",
        clean_text("empresa_operadora")
    )

    .withColumn(
        "departamento_original",
        F.col("departamento")
    )

    .withColumn(
        "departamento",
        normalize_departamento("departamento")
    )

    .withColumn(
        "servicio_involucrado",
        clean_text("servicio_involucrado")
    )

    .withColumn(
        "forma_presentacion",
        clean_text("forma_presentacion")
    )

    .withColumn(
        "materia_reclamable",
        clean_text("materia_reclamable")
    )

    # --------------------------------------------------------
    # Periodo
    # Bronze puede contener timestamp.
    # Silver lo deja como DATE y al primer día del mes.
    # --------------------------------------------------------
    .withColumn(
        "mes",
        F.trunc(
            F.to_date(F.col("mes")),
            "month"
        )
    )

    # --------------------------------------------------------
    # Preparar cantidad para conversión numérica
    # Ejemplos:
    # "4,357"        -> "4357"
    # "27.00000000"  -> decimal -> 27
    # --------------------------------------------------------
    .withColumn(
        "_cantidad_texto",
        F.regexp_replace(
            F.trim(
                F.col("cantidad_reclamos_presentados")
                .cast("string")
            ),
            ",",
            ""
        )
    )

    # TRY_CAST evita romper todo el proceso si aparece
    # un valor no convertible.
    .withColumn(
        "_cantidad_decimal",
        F.expr(
            """
            try_cast(
                _cantidad_texto
                AS DECIMAL(38,10)
            )
            """
        )
    )

    # --------------------------------------------------------
    # Metadato Silver
    # --------------------------------------------------------
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )
)

# ============================================================
# 5. REGLAS DE CALIDAD
# ============================================================

invalid_empresa = (
    F.col("empresa_operadora").isNull()
)

invalid_mes = (
    F.col("mes").isNull()
)

invalid_cantidad = (
    F.col("_cantidad_decimal").isNull()
    | (F.col("_cantidad_decimal") < 0)
    | (
        F.col("_cantidad_decimal")
        != F.floor(F.col("_cantidad_decimal"))
    )
)

invalid_record = (
    invalid_empresa
    | invalid_mes
    | invalid_cantidad
)


# ============================================================
# 6. REGISTROS RECHAZADOS
# ============================================================

df_rejected = (
    df_clean
    .filter(invalid_record)
    .withColumn(
        "_dq_reason",
        F.concat_ws(
            "; ",
            F.when(
                invalid_empresa,
                F.lit("empresa_operadora faltante")
            ),
            F.when(
                invalid_mes,
                F.lit("mes inválido o faltante")
            ),
            F.when(
                invalid_cantidad,
                F.lit("cantidad de reclamos inválida")
            )
        )
    )
)

rejected_count = df_rejected.count()

print(f"Registros rechazados: {rejected_count}")

if rejected_count > 0:
    display(
        df_rejected
        .select(
            "empresa_operadora_original",
            "mes",
            "departamento",
            "servicio_involucrado",
            "forma_presentacion",
            "materia_reclamable",
            "cantidad_reclamos_presentados",
            "_dq_reason"
        )
        .limit(50)
    )

# ============================================================
# 7. REGISTROS VÁLIDOS
# ============================================================

df_valid = (
    df_clean
    .filter(~invalid_record)

    # Convertir finalmente la medida a entero
    .withColumn(
        "cantidad_reclamos_presentados",
        F.col("_cantidad_decimal").cast("long")
    )

    # Campos auxiliares ya no son necesarios
    .drop(
        "_cantidad_texto",
        "_cantidad_decimal"
    )
)

valid_count = df_valid.count()

print(
    "Registros válidos:",
    valid_count
)
# ============================================================
# 8. AUDITORÍA DE POSIBLES DUPLICADOS
# ============================================================

business_columns = [
    "empresa_operadora",
    "mes",
    "departamento",
    "servicio_involucrado",
    "forma_presentacion",
    "materia_reclamable",
    "cantidad_reclamos_presentados"
]
# Silver conserva todos los registros válidos.
# Los posibles duplicados se detectan y auditan,
# pero no se eliminan sin evidencia de que sean errores.
df_silver = df_valid

silver_count = df_silver.count()

exact_duplicate_rows = (
    df_valid.count()
    - df_valid.dropDuplicates(business_columns).count()
)

print(
    f"Posibles filas duplicadas detectadas: {exact_duplicate_rows}"
)

# ============================================================
# 9. VALIDACIÓN DEL GRANO
# ============================================================

grain_columns = [
    "empresa_operadora",
    "mes",
    "departamento",
    "servicio_involucrado",
    "forma_presentacion",
    "materia_reclamable"
]

df_grain_duplicates = (
    df_silver
    .groupBy(*grain_columns)
    .agg(
        F.count("*").alias("cantidad_filas"),
        F.sum(
            "cantidad_reclamos_presentados"
        ).alias("suma_reclamos")
    )
    .filter(
        F.col("cantidad_filas") > 1
    )
)

grain_duplicate_groups = (
    df_grain_duplicates.count()
)

print(
    "Grupos repetidos según el grano:",
    grain_duplicate_groups
)

if grain_duplicate_groups > 0:
    display(
        df_grain_duplicates.limit(50)
    )


# ============================================================
# 10. SELECCIÓN FINAL DE COLUMNAS
# ============================================================

df_silver = df_silver.select(
    "empresa_operadora_original",
    "empresa_operadora",
    "mes",
    "departamento",
    "servicio_involucrado",
    "forma_presentacion",
    "materia_reclamable",
    "cantidad_reclamos_presentados",
    "_source_file",
    "_source_dataset",
    "_source_sheet",
    "_ingested_at",
    "_silver_processed_at"
)


# ============================================================
# 11. PERSISTENCIA SILVER
# ============================================================

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(
    f"Tabla Silver creada correctamente: {target_table}"
)



# ============================================================
# 12. PERSISTENCIA DE RECHAZADOS
# ============================================================

if rejected_count > 0:

    (
        df_rejected.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(reject_table)
    )

    print(
        f"Tabla de rechazados creada: {reject_table}"
    )

else:

    spark.sql(
        f"DROP TABLE IF EXISTS {reject_table}"
    )

    print("No existen registros rechazados.")



# ============================================================
# 13. RECONCILIACIÓN
# ============================================================

silver_count = df_silver.count()

print("")
print("==============================================")
print("RESUMEN DE CALIDAD")
print("==============================================")

print(f"Bronze:                         {bronze_count}")
print(f"Rechazados:                     {rejected_count}")
print(f"Posibles duplicados detectados: {exact_duplicate_rows}")
print(f"Silver:                         {silver_count}")
print(f"Grupos repetidos por grano:     {grain_duplicate_groups}")

assert bronze_count == silver_count + rejected_count, (
    "La reconciliación Bronze -> Silver no coincide."
)

print("Reconciliación correcta.")

# ============================================================
# 14. RESULTADO SILVER
# ============================================================

display(
    spark.sql("""
        SELECT *
        FROM osiptel_lakehouse.silver.reclamos_presentados
        LIMIT 20
    """)
)

spark.table(target_table).printSchema()

