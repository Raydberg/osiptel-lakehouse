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

def normalize_operador(column_name):
    """
    Homologa variantes ortográficas de operadores.
    La comparación ignora tildes y ñ, pero el resultado
    conserva una denominación canónica.
    """

    cleaned = clean_text(column_name)

    comparison_key = F.translate(
        cleaned,
        "ÁÉÍÓÚÜÑ",
        "AEIOUUN"
    )

    canonical_map = F.create_map(
        F.lit("AMERICA MOVIL PERU S.A.C."),
        F.lit("AMÉRICA MÓVIL PERÚ S.A.C."),

        F.lit("BASILES AREVALO FLORES"),
        F.lit("BASILES ARÉVALO FLORES"),

        F.lit("CABLE JAEN S.R.L."),
        F.lit("CABLE JAÉN S.R.L."),

        F.lit("COMPANIA TELEFONICA ANDINA S.A."),
        F.lit("COMPAÑÍA TELEFÓNICA ANDINA S.A."),

        F.lit("COMUNICACIONES PORCON S.A.C."),
        F.lit("COMUNICACIONES PORCÓN S.A.C."),

        F.lit("DEMETRIO RIGOBERTO PICON ACOSTA"),
        F.lit("DEMETRIO RIGOBERTO PICÓN ACOSTA"),

        F.lit("ENTEL PERU S.A."),
        F.lit("ENTEL PERÚ S.A."),

        F.lit("FLASH SERVICIOS PERU S.R.L."),
        F.lit("FLASH SERVICIOS PERÚ S.R.L."),

        F.lit("FLOR DE MARIA GUERRA VELA"),
        F.lit("FLOR DE MARÍA GUERRA VELA"),

        F.lit("INTEGRATEL PERU S.A.A."),
        F.lit("INTEGRATEL PERÚ S.A.A."),

        F.lit("SUMA MOVIL PERU S.A.C."),
        F.lit("SUMA MÓVIL PERÚ S.A.C."),

        F.lit("TELEFONICA DEL PERU S.A.A."),
        F.lit("TELEFÓNICA DEL PERÚ S.A.A."),

        F.lit("VIETTEL PERU S.A.C."),
        F.lit("VIETTEL PERÚ S.A.C.")
    )

    return F.coalesce(
        F.element_at(canonical_map, comparison_key),
        cleaned
    )


