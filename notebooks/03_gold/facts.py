from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURACIÓN
# ============================================================

gold_schema = "osiptel_lakehouse.gold"
silver_schema = "osiptel_lakehouse.silver"


# ============================================================
# 2. CARGA SILVER
# ============================================================

presentados = spark.table(
    f"{silver_schema}.reclamos_presentados"
)

resueltos = spark.table(
    f"{silver_schema}.reclamos_resueltos"
)

averias = spark.table(
    f"{silver_schema}.reclamos_averias"
)

lineas = spark.table(
    f"{silver_schema}.lineas_moviles"
)


# ============================================================
# 3. CARGA DE DIMENSIONES
# ============================================================

dim_tiempo = spark.table(
    f"{gold_schema}.dim_tiempo"
)

dim_operador = spark.table(
    f"{gold_schema}.dim_operador"
)

dim_geografia = spark.table(
    f"{gold_schema}.dim_geografia"
)

dim_servicio = spark.table(
    f"{gold_schema}.dim_servicio"
)

dim_materia = spark.table(
    f"{gold_schema}.dim_materia"
)

dim_forma = spark.table(
    f"{gold_schema}.dim_forma_presentacion"
)

dim_resultado = spark.table(
    f"{gold_schema}.dim_resultado"
)

dim_tipo_reclamo = spark.table(
    f"{gold_schema}.dim_tipo_reclamo"
)

dim_modalidad = spark.table(
    f"{gold_schema}.dim_modalidad"
)

print("Fuentes Silver y dimensiones Gold cargadas.")

# ============================================================
# 4. FACT_RECLAMOS_PRESENTADOS
# ============================================================

fact_presentados = (
    presentados

    .join(
        dim_tiempo,
        presentados.mes == dim_tiempo.fecha,
        "left"
    )

    .join(
        dim_operador,
        "empresa_operadora",
        "left"
    )

    .join(
        dim_geografia,
        "departamento",
        "left"
    )

    .join(
        dim_servicio,
        "servicio_involucrado",
        "left"
    )

    .join(
        dim_materia,
        "materia_reclamable",
        "left"
    )

    .join(
        dim_forma,
        "forma_presentacion",
        "left"
    )

    .select(
        "tiempo_key",
        "operador_key",
        F.coalesce(
            F.col("geografia_key"),
            F.lit(0)
        ).alias("geografia_key"),
        F.coalesce(
            F.col("servicio_key"),
            F.lit(0)
        ).alias("servicio_key"),
        F.coalesce(
            F.col("materia_key"),
            F.lit(0)
        ).alias("materia_key"),
        F.coalesce(
            F.col("forma_presentacion_key"),
            F.lit(0)
        ).alias("forma_presentacion_key"),
        "cantidad_reclamos_presentados"
    )

    .groupBy(
        "tiempo_key",
        "operador_key",
        "geografia_key",
        "servicio_key",
        "materia_key",
        "forma_presentacion_key"
    )

    .agg(
        F.sum(
            "cantidad_reclamos_presentados"
        ).alias("cantidad_reclamos_presentados")
    )
)




# ============================================================
# 5. FACT_RECLAMOS_RESUELTOS
# ============================================================

resueltos_gold = (
    resueltos
    .withColumn(
        "forma_presentacion",
        F.when(
            F.col("medio_presentacion") == "CORREO",
            F.lit("CORREO ELECTRÓNICO")
        )
        .otherwise(
            F.col("medio_presentacion")
        )
    )
)


fact_resueltos = (
    resueltos_gold

    .join(
        dim_tiempo,
        resueltos_gold.mes == dim_tiempo.fecha,
        "left"
    )

    .join(
        dim_operador,
        "empresa_operadora",
        "left"
    )

    .join(
        dim_geografia,
        "departamento",
        "left"
    )

    .join(
        dim_servicio,
        "servicio_involucrado",
        "left"
    )

    .join(
        dim_materia,
        "materia_reclamable",
        "left"
    )

    .join(
        dim_forma,
        "forma_presentacion",
        "left"
    )

    .join(
        dim_resultado,
        resueltos_gold.sentido_resolucion
        == dim_resultado.resultado,
        "left"
    )

    .select(
        "tiempo_key",
        "operador_key",
        F.coalesce(
            F.col("geografia_key"),
            F.lit(0)
        ).alias("geografia_key"),
        F.coalesce(
            F.col("servicio_key"),
            F.lit(0)
        ).alias("servicio_key"),
        F.coalesce(
            F.col("materia_key"),
            F.lit(0)
        ).alias("materia_key"),
        F.coalesce(
            F.col("forma_presentacion_key"),
            F.lit(0)
        ).alias("forma_presentacion_key"),
        F.coalesce(
            F.col("resultado_key"),
            F.lit(0)
        ).alias("resultado_key"),
        "cantidad_reclamos_resueltos"
    )

    .groupBy(
        "tiempo_key",
        "operador_key",
        "geografia_key",
        "servicio_key",
        "materia_key",
        "forma_presentacion_key",
        "resultado_key"
    )

    .agg(
        F.sum(
            "cantidad_reclamos_resueltos"
        ).alias("cantidad_reclamos_resueltos")
    )
)

# ============================================================
# 6. FACT_AVERIAS
# ============================================================

fact_averias = (
    averias

    .join(
        dim_tiempo,
        averias.mes == dim_tiempo.fecha,
        "left"
    )

    .join(
        dim_operador,
        "empresa_operadora",
        "left"
    )

    .join(
        dim_geografia,
        "departamento",
        "left"
    )

    .join(
        dim_servicio,
        "servicio_involucrado",
        "left"
    )

    .join(
        dim_materia,
        "materia_reclamable",
        "left"
    )

    .join(
        dim_tipo_reclamo,
        "tipo_reclamo",
        "left"
    )

    .select(
        "tiempo_key",
        "operador_key",
        F.coalesce(
            F.col("geografia_key"),
            F.lit(0)
        ).alias("geografia_key"),
        F.coalesce(
            F.col("servicio_key"),
            F.lit(0)
        ).alias("servicio_key"),
        F.coalesce(
            F.col("materia_key"),
            F.lit(0)
        ).alias("materia_key"),
        F.coalesce(
            F.col("tipo_reclamo_key"),
            F.lit(0)
        ).alias("tipo_reclamo_key"),
        "cantidad_reclamos_averias"
    )

    .groupBy(
        "tiempo_key",
        "operador_key",
        "geografia_key",
        "servicio_key",
        "materia_key",
        "tipo_reclamo_key"
    )

    .agg(
        F.sum(
            "cantidad_reclamos_averias"
        ).alias("cantidad_reclamos_averias")
    )
)


# ============================================================
# 7. FACT_LINEAS
# ============================================================

fact_lineas = (
    lineas

    .join(
        dim_tiempo,
        lineas.mes == dim_tiempo.fecha,
        "left"
    )

    .join(
        dim_operador,
        "empresa_operadora",
        "left"
    )

    .join(
        dim_geografia,
        "departamento",
        "left"
    )

    .join(
        dim_modalidad,
        "modalidad_contractual",
        "left"
    )

    .select(
        "tiempo_key",
        "operador_key",
        F.coalesce(
            F.col("geografia_key"),
            F.lit(0)
        ).alias("geografia_key"),
        F.coalesce(
            F.col("modalidad_key"),
            F.lit(0)
        ).alias("modalidad_key"),
        "cantidad_lineas"
    )

    .groupBy(
        "tiempo_key",
        "operador_key",
        "geografia_key",
        "modalidad_key"
    )

    .agg(
        F.sum(
            "cantidad_lineas"
        ).alias("cantidad_lineas")
    )
)




# ============================================================
# 8. PERSISTENCIA GOLD
# ============================================================

facts = {
    "fact_reclamos_presentados": fact_presentados,
    "fact_reclamos_resueltos": fact_resueltos,
    "fact_averias": fact_averias,
    "fact_lineas": fact_lineas
}


for table_name, dataframe in facts.items():

    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(
            f"{gold_schema}.{table_name}"
        )
    )

    print(
        f"{table_name}: "
        f"{dataframe.count()} registros"
    )

