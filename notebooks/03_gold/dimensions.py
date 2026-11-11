from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# 1. CONFIGURACIÓN
# ============================================================

gold_schema = "osiptel_lakehouse.gold"

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {gold_schema}"
)


# ============================================================
# 2. CARGA DE TABLAS SILVER
# ============================================================

df_presentados = spark.table(
    "osiptel_lakehouse.silver.reclamos_presentados"
)

df_resueltos = spark.table(
    "osiptel_lakehouse.silver.reclamos_resueltos"
)

df_averias = spark.table(
    "osiptel_lakehouse.silver.reclamos_averias"
)

df_lineas = spark.table(
    "osiptel_lakehouse.silver.lineas_moviles"
)


print("Tablas Silver cargadas correctamente.")


# ============================================================
# 3. FUNCIÓN GENERAL PARA DIMENSIONES
# ============================================================

def create_dimension(
    df_values,
    value_column,
    key_column,
    target_table
):
    # Valores reales únicos
    df_values = (
        df_values
        .select(
            F.col(value_column).alias(value_column)
        )
        .filter(
            F.col(value_column).isNotNull()
        )
        .distinct()
    )

    # Claves sustitutas determinísticas
    window_spec = Window.orderBy(value_column)

    df_dimension = (
        df_values
        .withColumn(
            key_column,
            F.row_number().over(window_spec)
        )
        .select(
            key_column,
            value_column
        )
    )

    # Miembro desconocido
    df_unknown = spark.createDataFrame(
        [(0, "SIN INFORMACIÓN")],
        schema=f"{key_column} INT, {value_column} STRING"
    )

    df_dimension = (
        df_unknown
        .unionByName(df_dimension)
    )

    # Persistencia
    (
        df_dimension.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    print(
        f"{target_table}: "
        f"{df_dimension.count()} registros"
    )

    return df_dimension

# ============================================================
# 4. DIM_TIEMPO
# ============================================================

df_fechas = (
    df_presentados.select("mes")
    .union(df_resueltos.select("mes"))
    .union(df_averias.select("mes"))
    .union(df_lineas.select("mes"))
    .filter(F.col("mes").isNotNull())
    .distinct()
)


dim_tiempo = (
    df_fechas

    .withColumn(
        "tiempo_key",
        (
            F.year("mes") * 100
            + F.month("mes")
        ).cast("int")
    )

    .withColumn(
        "anio",
        F.year("mes")
    )

    .withColumn(
        "mes_numero",
        F.month("mes")
    )

    .withColumn(
        "mes_nombre",
        F.date_format("mes", "MMMM")
    )

    .withColumn(
        "trimestre",
        F.quarter("mes")
    )

    .withColumn(
        "semestre",
        F.when(
            F.month("mes") <= 6,
            F.lit(1)
        ).otherwise(
            F.lit(2)
        )
    )

    .withColumn(
        "anio_mes",
        F.date_format(
            "mes",
            "yyyy-MM"
        )
    )

    .select(
        "tiempo_key",
        F.col("mes").alias("fecha"),
        "anio",
        "mes_numero",
        "mes_nombre",
        "trimestre",
        "semestre",
        "anio_mes"
    )

    .orderBy("fecha")
)


(
    dim_tiempo.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{gold_schema}.dim_tiempo"
    )
)

print(
    "DIM_TIEMPO:",
    dim_tiempo.count(),
    "registros"
)

display(dim_tiempo)



# ============================================================
# 5. DIM_OPERADOR
# ============================================================

df_operadores = (
    df_presentados
        .select("empresa_operadora")

    .union(
        df_resueltos
        .select("empresa_operadora")
    )

    .union(
        df_averias
        .select("empresa_operadora")
    )

    .union(
        df_lineas
        .select("empresa_operadora")
    )
)


dim_operador = create_dimension(
    df_operadores,
    "empresa_operadora",
    "operador_key",
    f"{gold_schema}.dim_operador"
)

display(dim_operador)



# ============================================================
# 6. DIM_GEOGRAFIA
# ============================================================

df_geografias = (
    df_presentados
        .select("departamento")

    .union(
        df_resueltos
        .select("departamento")
    )

    .union(
        df_averias
        .select("departamento")
    )

    .union(
        df_lineas
        .select("departamento")
    )
)


dim_geografia = create_dimension(
    df_geografias,
    "departamento",
    "geografia_key",
    f"{gold_schema}.dim_geografia"
)

display(dim_geografia)



# ============================================================
# 7. DIM_SERVICIO
# ============================================================

df_servicios = (
    df_presentados
        .select("servicio_involucrado")

    .union(
        df_resueltos
        .select("servicio_involucrado")
    )

    .union(
        df_averias
        .select("servicio_involucrado")
    )
)


dim_servicio = create_dimension(
    df_servicios,
    "servicio_involucrado",
    "servicio_key",
    f"{gold_schema}.dim_servicio"
)

display(dim_servicio)


# ============================================================
# 8. DIM_MATERIA
# ============================================================

df_materias = (
    df_presentados
        .select("materia_reclamable")

    .union(
        df_resueltos
        .select("materia_reclamable")
    )

    .union(
        df_averias
        .select("materia_reclamable")
    )
)


dim_materia = create_dimension(
    df_materias,
    "materia_reclamable",
    "materia_key",
    f"{gold_schema}.dim_materia"
)

display(dim_materia)



# ============================================================
# 9. DIM_FORMA_PRESENTACION
# ============================================================

df_forma_presentados = (
    df_presentados
    .select(
        F.col(
            "forma_presentacion"
        ).alias("forma_presentacion")
    )
)


df_forma_resueltos = (
    df_resueltos
    .select(
        F.when(
            F.col("medio_presentacion") == "CORREO",
            F.lit("CORREO ELECTRÓNICO")
        )
        .otherwise(
            F.col("medio_presentacion")
        )
        .alias("forma_presentacion")
    )
)


df_formas = (
    df_forma_presentados
    .union(df_forma_resueltos)
)


dim_forma_presentacion = create_dimension(
    df_formas,
    "forma_presentacion",
    "forma_presentacion_key",
    f"{gold_schema}.dim_forma_presentacion"
)

display(dim_forma_presentacion)

# ============================================================
# 10. DIM_RESULTADO
# ============================================================

dim_resultado = create_dimension(
    df_resueltos.select(
        F.col(
            "sentido_resolucion"
        ).alias("resultado")
    ),
    "resultado",
    "resultado_key",
    f"{gold_schema}.dim_resultado"
)

display(dim_resultado)


# ============================================================
# 11. DIM_TIPO_RECLAMO
# ============================================================

dim_tipo_reclamo = create_dimension(
    df_averias.select(
        "tipo_reclamo"
    ),
    "tipo_reclamo",
    "tipo_reclamo_key",
    f"{gold_schema}.dim_tipo_reclamo"
)

display(dim_tipo_reclamo)


# ============================================================
# 12. DIM_MODALIDAD
# ============================================================

dim_modalidad = create_dimension(
    df_lineas.select(
        "modalidad_contractual"
    ),
    "modalidad_contractual",
    "modalidad_key",
    f"{gold_schema}.dim_modalidad"
)

display(dim_modalidad)


# ============================================================
# 13. VALIDACIÓN FINAL
# ============================================================

dimension_tables = [
    "dim_tiempo",
    "dim_operador",
    "dim_geografia",
    "dim_servicio",
    "dim_materia",
    "dim_forma_presentacion",
    "dim_resultado",
    "dim_tipo_reclamo",
    "dim_modalidad"
]

print("")
print("==============================================")
print("DIMENSIONES GOLD CREADAS")
print("==============================================")

for table in dimension_tables:

    count = spark.table(
        f"{gold_schema}.{table}"
    ).count()

    print(
        f"{table}: {count} registros"
    )

