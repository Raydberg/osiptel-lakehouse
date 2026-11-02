# OSIPTEL Lakehouse

Plataforma analítica para la integración y análisis de datos abiertos del sector de telecomunicaciones en el Perú.

El proyecto implementa una solución de Business Intelligence basada en una arquitectura Lakehouse utilizando Databricks, Apache Spark, Delta Lake y Power BI.

## Objetivo

Integrar, transformar y analizar diferentes conjuntos de datos abiertos publicados por OSIPTEL con el propósito de generar información analítica que permita identificar patrones, tendencias y diferencias entre empresas operadoras, servicios, periodos y ubicaciones geográficas.

## Fuentes de datos

El alcance del proyecto considera cuatro conjuntos de datos de OSIPTEL:

1. Reclamos presentados
2. Reclamos resueltos
3. Reclamos por averías
4. Líneas móviles

No se incorporarán nuevas fuentes al alcance principal sin una revisión previa del diseño y de su impacto en el modelo analítico.

## Arquitectura

La solución utiliza una arquitectura Lakehouse basada en el patrón Medallion:

```text
Fuentes OSIPTEL
      │
      ▼
    Ingesta
      │
      ▼
    Bronze
 Datos originales
      │
      ▼
    Silver
 Datos limpios,
 normalizados e integrados
      │
      ▼
     Gold
 Modelo dimensional
 + indicadores
      │
      ▼
Databricks SQL Warehouse
      │
      ▼
   Power BI
```

### Bronze

Conserva los datos provenientes de las fuentes seleccionadas con el mínimo nivel de transformación necesario para su almacenamiento y trazabilidad.

### Silver

Contiene los datos después de aplicar procesos de calidad, entre ellos:

- validación de tipos de datos;
- tratamiento de valores nulos;
- eliminación de duplicados;
- normalización de empresas operadoras;
- normalización de periodos;
- normalización de servicios;
- normalización de ubicaciones geográficas.

### Gold

Contiene las estructuras analíticas orientadas al consumo mediante herramientas de Business Intelligence.

El modelo dimensional considera inicialmente tablas de hechos asociadas a:

- reclamos presentados;
- reclamos resueltos;
- reclamos por averías;
- líneas móviles.

Las dimensiones serán refinadas después del perfilamiento de las fuentes.

## Tecnologías

- Databricks
- Apache Spark
- PySpark
- Spark SQL
- Delta Lake
- Databricks SQL
- Power BI
- GitHub

## Estructura del repositorio

```text
config/
    Configuración y definición de las fuentes.

notebooks/
    Notebooks utilizados para ejecución, exploración y validación.

src/
    Código Python reutilizable para ingesta, transformación y calidad de datos.

tests/
    Pruebas automatizadas de transformaciones y reglas de calidad.
```

La lógica reutilizable debe implementarse principalmente dentro de `src/`. Los notebooks deben utilizarse como capas de ejecución, exploración y visualización.

## Flujo de procesamiento

```text
RAW
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

## Convenciones de tablas

Las tablas seguirán una nomenclatura basada en la capa correspondiente.

Ejemplos:

```text
bronze.reclamos_presentados
bronze.reclamos_resueltos
bronze.reclamos_averias
bronze.lineas_moviles

silver.reclamos_presentados
silver.reclamos_resueltos
silver.reclamos_averias
silver.lineas_moviles

gold.fact_reclamos_presentados
gold.fact_reclamos_resueltos
gold.fact_averias
gold.fact_lineas

gold.dim_tiempo
gold.dim_operador
gold.dim_servicio
gold.dim_geografia
```

## Gestión de datos

Los archivos originales y datasets procesados no deben almacenarse directamente en GitHub.

GitHub se utilizará para versionar:

- código;
- notebooks;
- configuraciones;
- documentación;
- pruebas.

Los datos serán almacenados y procesados dentro de Databricks.

## Flujo de trabajo Git

La rama `main` contiene la versión estable del proyecto.

El desarrollo de nuevas funcionalidades debe realizarse mediante ramas independientes, por ejemplo:

```text
feature/bronze-ingestion
feature/silver-cleaning
feature/gold-model
feature/powerbi
```

Los cambios deben integrarse mediante revisión antes de incorporarse a `main`.
