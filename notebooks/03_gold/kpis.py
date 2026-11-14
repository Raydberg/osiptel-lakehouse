from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURACIÓN
# ============================================================

gold_schema = "osiptel_lakehouse.gold"


# ============================================================
# 2. FACTS
# ============================================================

fact_presentados = spark.table(
    f"{gold_schema}.fact_reclamos_presentados"
)

fact_resueltos = spark.table(
    f"{gold_schema}.fact_reclamos_resueltos"
)

fact_averias = spark.table(
    f"{gold_schema}.fact_averias"
)

fact_lineas = spark.table(
    f"{gold_schema}.fact_lineas"
)


# ============================================================
# 3. DIMENSIONES
# ============================================================

dim_tiempo = spark.table(
    f"{gold_schema}.dim_tiempo"
)

dim_operador = spark.table(
    f"{gold_schema}.dim_operador"
)

dim_servicio = spark.table(
    f"{gold_schema}.dim_servicio"
)

dim_resultado = spark.table(
    f"{gold_schema}.dim_resultado"
)

dim_tipo_reclamo = spark.table(
    f"{gold_schema}.dim_tipo_reclamo"
)

print("Facts y dimensiones Gold cargadas.")


# ============================================================
# 4. KPI RECLAMOS PRESENTADOS
# ============================================================

kpi_reclamos_presentados = (
    fact_presentados
    .groupBy(
        "tiempo_key",
        "operador_key"
    )
    .agg(
        F.sum(
            "cantidad_reclamos_presentados"
        ).alias("reclamos_presentados")
    )
)

display(kpi_reclamos_presentados.limit(20))



# ============================================================
# 5. KPI RECLAMOS RESUELTOS
# ============================================================

kpi_reclamos_resueltos = (
    fact_resueltos
    .groupBy(
        "tiempo_key",
        "operador_key"
    )
    .agg(
        F.sum(
            "cantidad_reclamos_resueltos"
        ).alias("reclamos_resueltos")
    )
)


# ============================================================
# 6. KPI RESULTADOS INFUNDADOS
# ============================================================

resueltos_resultado = (
    fact_resueltos
    .join(
        dim_resultado,
        "resultado_key",
        "left"
    )
)


kpi_infundados = (
    resueltos_resultado

    .groupBy(
        "tiempo_key",
        "operador_key"
    )

    .agg(
        F.sum(
            "cantidad_reclamos_resueltos"
        ).alias("total_resueltos"),

        F.sum(
            F.when(
                F.col("resultado") == "INFUNDADO",
                F.col("cantidad_reclamos_resueltos")
            )
            .otherwise(0)
        ).alias("resueltos_infundados")
    )

    .withColumn(
        "porcentaje_categoria_infundado",
        F.when(
            F.col("total_resueltos") > 0,
            F.round(
                (
                    F.col("resueltos_infundados")
                    / F.col("total_resueltos")
                ) * 100,
                2
            )
        )
    )
)



# ============================================================
# 7. KPI AVERÍAS
# ============================================================

averias_tipo = (
    fact_averias
    .join(
        dim_tipo_reclamo,
        "tipo_reclamo_key",
        "left"
    )
)


kpi_averias = (
    averias_tipo

    .groupBy(
        "tiempo_key",
        "operador_key"
    )

    .agg(
        F.sum(
            F.when(
                F.col("tipo_reclamo") == "PRESENTADOS",
                F.col("cantidad_reclamos_averias")
            )
            .otherwise(0)
        ).alias("averias_presentadas"),

        F.sum(
            F.when(
                F.col("tipo_reclamo") == "SOLUCIONADOS",
                F.col("cantidad_reclamos_averias")
            )
            .otherwise(0)
        ).alias("averias_solucionadas")
    )

    .withColumn(
        "relacion_solucionados_presentados_pct",
        F.when(
            F.col("averias_presentadas") > 0,
            F.round(
                (
                    F.col("averias_solucionadas")
                    / F.col("averias_presentadas")
                ) * 100,
                2
            )
        )
    )
)



# ============================================================
# 8. KPI LÍNEAS MÓVILES
# ============================================================

kpi_lineas = (
    fact_lineas

    .groupBy(
        "tiempo_key",
        "operador_key"
    )

    .agg(
        F.sum(
            "cantidad_lineas"
        ).alias("lineas_moviles")
    )
)

# ============================================================
# 9. RECLAMOS DEL SERVICIO MÓVIL
# ============================================================

presentados_servicio = (
    fact_presentados
    .join(
        dim_servicio,
        "servicio_key",
        "left"
    )
)


reclamos_moviles = (
    presentados_servicio

    .filter(
        F.col("servicio_involucrado")
        == "SERVICIOS MÓVILES"
    )

    .groupBy(
        "tiempo_key",
        "operador_key"
    )

    .agg(
        F.sum(
            "cantidad_reclamos_presentados"
        ).alias("reclamos_moviles")
    )
)



# ============================================================
# 10. INCIDENCIA DE RECLAMOS MÓVILES
# ============================================================

kpi_incidencia_movil = (
    reclamos_moviles

    .join(
        kpi_lineas,
        [
            "tiempo_key",
            "operador_key"
        ],
        "inner"
    )

    .withColumn(
        "reclamos_por_1000_lineas",
        F.when(
            F.col("lineas_moviles") > 0,
            F.round(
                (
                    F.col("reclamos_moviles")
                    / F.col("lineas_moviles")
                ) * 1000,
                2
            )
        )
    )
)
# ============================================================
# 11. PERSISTENCIA
# ============================================================

kpis = {
    "kpi_reclamos_presentados": kpi_reclamos_presentados,
    "kpi_reclamos_resueltos": kpi_reclamos_resueltos,
    "kpi_infundados": kpi_infundados,
    "kpi_averias": kpi_averias,
    "kpi_lineas": kpi_lineas,
    "kpi_incidencia_movil": kpi_incidencia_movil
}


for table_name, dataframe in kpis.items():

    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            f"{gold_schema}.{table_name}"
        )
    )

    print(
        f"{table_name}: "
        f"{dataframe.count()} registros"
    )
