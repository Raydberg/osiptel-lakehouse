from pyspark.sql import functions as F

from shared.cleaning import (
    clean_text,
    normalize_departamento
)

# ============================================================
# 1. CONFIGURACIÓN
# ============================================================

source_table = "osiptel_lakehouse.bronze.lineas_moviles"
target_table = "osiptel_lakehouse.silver.lineas_moviles"
reject_table = "osiptel_lakehouse.silver.lineas_moviles_rechazados"

spark.sql("CREATE SCHEMA IF NOT EXISTS osiptel_lakehouse.silver")


# ============================================================
# 2. LECTURA DE BRONZE
# ============================================================

df_bronze = spark.table(source_table)

# Compatibilidad con Bronze antiguos
if "_source_sheet" not in df_bronze.columns:
    df_bronze = df_bronze.withColumn(
        "_source_sheet",
        F.lit("Dataset")
    )

bronze_count = df_bronze.count()

print("==============================================")
print("SILVER - LÍNEAS MÓVILES")
print("==============================================")
print(f"Registros recibidos desde Bronze: {bronze_count}")

display(df_bronze.limit(20))
df_bronze.printSchema()


# ============================================================
# 4. LIMPIEZA Y ESTANDARIZACIÓN
# ============================================================

df_clean = (
    df_bronze

    # Mantener denominación original del operador
    .withColumn(
        "empresa_operadora_original",
        F.col("empresa_operadora")
    )

    # Normalización textual
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
        "modalidad_contractual",
        clean_text("modalidad_contractual")
    )

    # Mes como DATE al primer día del mes
    .withColumn(
        "mes",
        F.trunc(
            F.to_date(F.col("mes")),
            "month"
        )
    )

    # --------------------------------------------------------
    # Cantidad de líneas
    #
    # "898,173" -> "898173"
    # "22000"   -> "22000"
    # ""        -> NULL
    # --------------------------------------------------------
    .withColumn(
        "_cantidad_texto",
        F.regexp_replace(
            F.trim(
                F.col("cantidad_lineas")
                .cast("string")
            ),
            ",",
            ""
        )
    )

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

    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )
)


# ============================================================
# 5. REGLAS DE CALIDAD
# ============================================================

# Empresa, mes y cantidad son necesarios
# para utilizar analíticamente una observación.

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
                F.lit("cantidad de líneas inválida")
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
            "departamento",
            "modalidad_contractual",
            "mes",
            "cantidad_lineas",
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

    .withColumn(
        "cantidad_lineas",
        F.col("_cantidad_decimal").cast("long")
    )

    .drop(
        "_cantidad_texto",
        "_cantidad_decimal"
    )
)

valid_count = df_valid.count()

print(f"Registros válidos: {valid_count}")


# ============================================================
# 8. AUDITORÍA DE POSIBLES DUPLICADOS
# ============================================================

business_columns = [
    "empresa_operadora",
    "departamento",
    "modalidad_contractual",
    "mes",
    "cantidad_lineas"
]

# No eliminamos automáticamente.
df_silver = df_valid

exact_duplicate_rows = (
    df_valid.count()
    - df_valid
        .dropDuplicates(business_columns)
        .count()
)

print(
    f"Posibles filas duplicadas detectadas: "
    f"{exact_duplicate_rows}"
)


# ============================================================
# 9. VALIDACIÓN DEL GRANO
# ============================================================

# Grano analítico esperado:
# operador + mes + departamento + modalidad

grain_columns = [
    "empresa_operadora",
    "mes",
    "departamento",
    "modalidad_contractual"
]

df_grain_duplicates = (
    df_silver
    .groupBy(*grain_columns)
    .agg(
        F.count("*").alias("cantidad_filas"),

        F.countDistinct(
            "cantidad_lineas"
        ).alias("cantidades_distintas"),

        F.sum(
            "cantidad_lineas"
        ).alias("suma_lineas")
    )
    .filter(
        F.col("cantidad_filas") > 1
    )
)

grain_duplicate_groups = (
    df_grain_duplicates.count()
)

print(
    f"Grupos repetidos según el grano: "
    f"{grain_duplicate_groups}"
)

if grain_duplicate_groups > 0:

    display(
        df_grain_duplicates
        .orderBy(
            F.desc("cantidad_filas")
        )
        .limit(50)
    )


# ============================================================
# 10. PERFILAMIENTO DE NULOS
# ============================================================

print("")
print("==============================================")
print("NULOS DESPUÉS DE LA LIMPIEZA")
print("==============================================")

for column_name in [
    "departamento",
    "modalidad_contractual"
]:

    null_count = (
        df_silver
        .filter(
            F.col(column_name).isNull()
        )
        .count()
    )

    print(
        f"{column_name}: {null_count}"
    )


# ============================================================
# 11. DOMINIO DE MODALIDAD CONTRACTUAL
# ============================================================

print("")
print("Valores de modalidad_contractual:")

display(
    df_silver
    .groupBy(
        "modalidad_contractual"
    )
    .agg(
        F.count("*").alias("registros"),
        F.sum(
            "cantidad_lineas"
        ).alias("cantidad_lineas")
    )
    .orderBy(
        F.desc("registros")
    )
)


# ============================================================
# 12. DEPARTAMENTOS / UBICACIÓN
# ============================================================

print("")
print("Valores de departamento:")

display(
    df_silver
    .groupBy(
        "departamento"
    )
    .agg(
        F.count("*").alias("registros"),
        F.sum(
            "cantidad_lineas"
        ).alias("cantidad_lineas")
    )
    .orderBy(
        F.desc("registros")
    )
)


# ============================================================
# 13. SELECCIÓN FINAL
# ============================================================

df_silver = df_silver.select(
    "empresa_operadora_original",
    "empresa_operadora",
    "departamento_original",
    "departamento",
    "modalidad_contractual",
    "mes",
    "cantidad_lineas",
    "_source_file",
    "_source_dataset",
    "_source_sheet",
    "_ingested_at",
    "_silver_processed_at"
)


# ============================================================
# 14. PERSISTENCIA SILVER
# ============================================================

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(
    f"Tabla Silver creada correctamente: "
    f"{target_table}"
)


# ============================================================
# 15. PERSISTENCIA DE RECHAZADOS
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
        f"Tabla de rechazados creada: "
        f"{reject_table}"
    )

else:

    spark.sql(
        f"DROP TABLE IF EXISTS {reject_table}"
    )

    print(
        "No existen registros rechazados."
    )


# ============================================================
# 16. RECONCILIACIÓN BRONZE -> SILVER
# ============================================================

silver_count = df_silver.count()


bronze_total = (
    df_bronze
    .select(
        F.sum(
            F.expr(
                """
                cast(
                    try_cast(
                        regexp_replace(
                            trim(
                                cast(
                                    cantidad_lineas
                                    as string
                                )
                            ),
                            ',',
                            ''
                        )
                        as decimal(38,10)
                    )
                    as bigint
                )
                """
            )
        ).alias("total")
    )
    .first()["total"]
)


silver_total = (
    df_silver
    .agg(
        F.sum(
            "cantidad_lineas"
        ).alias("total")
    )
    .first()["total"]
)


print("")
print("==============================================")
print("RESUMEN DE CALIDAD")
print("==============================================")

print(
    f"Bronze:                         "
    f"{bronze_count}"
)

print(
    f"Silver:                         "
    f"{silver_count}"
)

print(
    f"Rechazados:                     "
    f"{rejected_count}"
)

print(
    f"Posibles duplicados detectados: "
    f"{exact_duplicate_rows}"
)

print(
    f"Grupos repetidos por grano:     "
    f"{grain_duplicate_groups}"
)

print(
    f"Total líneas Bronze:            "
    f"{bronze_total}"
)

print(
    f"Total líneas Silver:            "
    f"{silver_total}"
)


# ============================================================
# 17. ASSERTS DE CALIDAD
# ============================================================

assert bronze_count == silver_count + rejected_count, (
    "La reconciliación de registros Bronze -> Silver "
    "no coincide."
)

assert bronze_total == silver_total, (
    "La suma de líneas cambió durante la transformación."
)

print("")
print("Reconciliación correcta.")


# ============================================================
# 18. COBERTURA TEMPORAL
# ============================================================

display(
    df_silver.agg(
        F.min("mes").alias("periodo_minimo"),
        F.max("mes").alias("periodo_maximo"),
        F.count("*").alias("registros"),
        F.sum(
            "cantidad_lineas"
        ).alias("total_lineas")
    )
)


# ============================================================
# 19. RESULTADO SILVER
# ============================================================

display(
    spark.sql("""
        SELECT *
        FROM osiptel_lakehouse.silver.lineas_moviles
        LIMIT 20
    """)
)

spark.table(target_table).printSchema()