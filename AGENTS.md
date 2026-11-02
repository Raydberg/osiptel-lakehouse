# AGENTS.md

## Project

This repository implements an analytical Lakehouse for public telecommunications data published by OSIPTEL.

The main objective is to integrate and analyze telecommunications complaints using Databricks, Apache Spark, Delta Lake and Power BI.

## Project scope

The project currently uses exactly four datasets:

1. Reclamos presentados
2. Reclamos resueltos
3. Reclamos por averías
4. Líneas móviles

Do not add additional datasets or expand the functional scope unless explicitly requested.

## Architecture

The project follows a Medallion architecture:

```text
Sources
  ↓
Bronze
  ↓
Silver
  ↓
Gold
  ↓
Databricks SQL
  ↓
Power BI
```

### Bronze

Preserve source data with minimal transformation.

Do not introduce business rules in Bronze.

### Silver

Apply:

- schema validation;
- type conversion;
- null handling;
- duplicate handling;
- operator normalization;
- service normalization;
- geographic normalization;
- period normalization;
- integration of compatible datasets.

### Gold

Gold contains analytical structures intended for BI consumption.

Use dimensional modeling where appropriate.

Expected fact tables include:

```text
fact_reclamos_presentados
fact_reclamos_resueltos
fact_averias
fact_lineas
```

Expected shared dimensions may include:

```text
dim_tiempo
dim_operador
dim_servicio
dim_geografia
```

Additional dimensions must be justified by the source data and analytical requirements.

## Repository structure

```text
config/
notebooks/
src/
tests/
```

### src/

Core reusable business and transformation logic belongs in `src/`.

Prefer small, testable Python functions.

### notebooks/

Notebooks should be thin.

Use notebooks primarily for:

- orchestration;
- exploration;
- validation;
- visualization;
- invoking functions implemented in `src/`.

Avoid duplicating transformation logic across notebooks.

## Data rules

Never commit:

- raw datasets;
- generated datasets;
- CSV/XLSX/Parquet data exports;
- credentials;
- tokens;
- secrets;
- `.env` files.

Source datasets must remain outside Git and be accessed through the Databricks data storage layer.

Small synthetic test fixtures may be stored under `tests/fixtures/`.

## Naming conventions

Use lowercase snake_case for:

- Python files;
- functions;
- variables;
- tables;
- columns.

Examples:

```text
reclamos_presentados
normalizar_operador
fact_reclamos_resueltos
dim_geografia
```

Use descriptive names.

Avoid names such as:

```text
test2.py
final.py
prueba.py
temp.py
```

## PySpark conventions

Prefer Spark DataFrame transformations over row-by-row Python processing.

Avoid unnecessary:

```text
collect()
toPandas()
```

on production datasets.

Transformations should preferably return new DataFrames instead of relying on hidden global state.

## Data quality

Before moving data from Bronze to Silver, validate at minimum:

- expected columns;
- data types;
- required values;
- duplicate records;
- malformed periods;
- valid operator values.

Do not silently discard invalid data.

When practical, expose rejected or invalid records for inspection.

## Dimensional model

Do not create dimensions or fact tables only because their names appear useful.

Derive the model from:

1. analytical questions;
2. dataset grain;
3. measurable facts;
4. descriptive dimensions.

Every fact table must have a clearly defined grain.

Shared business concepts such as operator, time and geography should reuse conformed dimensions where compatible.

## Testing

Transformation logic placed in `src/` should be testable independently from notebooks.

Tests should cover important normalization and data-quality rules.

A transformation should not be considered complete if its expected behavior cannot be verified.

## Git workflow

`main` represents the stable version of the project.

Use feature branches for development.

Examples:

```text
feature/bronze-ingestion
feature/silver-cleaning
feature/gold-model
feature/data-quality
```

Do not overwrite unrelated work.

Before modifying shared transformations, inspect their downstream use.

## Documentation

Update `README.md` when changes affect:

- project scope;
- architecture;
- repository structure;
- setup;
- technologies;
- primary data sources.

Do not duplicate extensive documentation inside this file.

`AGENTS.md` should contain durable repository instructions, while `README.md` documents the project for human contributors.

## Important constraints

Do not:

- invent columns that are not present in the OSIPTEL datasets;
- infer causal relationships from descriptive data;
- claim real-time processing unless it is implemented;
- introduce Machine Learning as part of the mandatory scope;
- add additional datasets without explicit approval;
- modify the architecture without documenting the reason.

When source schemas are uncertain, inspect the source before designing transformations around assumed fields.