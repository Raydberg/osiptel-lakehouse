from pyspark.sql import functions as F


def clean_text(column_name):
    """
    Limpieza textual común:
    - elimina espacios iniciales/finales;
    - reduce espacios múltiples;
    - convierte vacío y '-' a NULL;
    - convierte texto a mayúsculas.
    """

    cleaned = F.regexp_replace(
        F.trim(F.col(column_name).cast("string")),
        r"\s+",
        " "
    )

    return (
        F.when(
            cleaned.isNull()
            | (cleaned == "")
            | (cleaned == "-"),
            F.lit(None)
        )
        .otherwise(F.upper(cleaned))
    )


def normalize_departamento(column_name):
    """
    Homologa variantes ortográficas de departamentos,
    utilizando una clave sin tildes para comparar
    y devolviendo la denominación canónica.
    """

    cleaned = clean_text(column_name)

    comparison_key = F.translate(
        cleaned,
        "ÁÉÍÓÚÜÑ",
        "AEIOUUN"
    )

    canonical_map = F.create_map(
        F.lit("ANCASH"), F.lit("ÁNCASH"),
        F.lit("APURIMAC"), F.lit("APURÍMAC"),
        F.lit("HUANUCO"), F.lit("HUÁNUCO"),
        F.lit("JUNIN"), F.lit("JUNÍN"),
        F.lit("SAN MARTIN"), F.lit("SAN MARTÍN"),
    )

    return F.coalesce(
        F.element_at(canonical_map, comparison_key),
        cleaned
    )