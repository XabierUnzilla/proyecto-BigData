<<<<<<< HEAD
# ATBD · Yellow Taxi 2025

Trabajo en grupo de ATBD: 10 analíticas con Spark DataFrame sobre los viajes de Yellow Taxi de Nueva York de 2025, ejecutadas en un clúster Hadoop (HDFS + YARN) en AWS Academy.

Equipo: Ane, Keltse, Xabi y Ainhoa.

## Estructura

```
data/
  yellow_taxi/             12 Parquet mensuales (no se suben a git)
  taxi_zone_lookup.csv     tabla de zonas de TLC
  meteo/                   meteorología diaria de NOAA (pendiente)
scripts/
  cargar_datos.py          lectura de Parquet y CSV, esquema común
  preparar_datos.py        columnas derivadas, limpieza y join con zonas
  consultas.py             q01 … q10, una función por consulta
  main.py                  ejecuta las consultas y guarda cada resultado
exploracion/
  explorar_enero.py        comprobaciones de la etapa 1 sobre enero
documentos/                documento de resultados
```

## Puesta en marcha en local

Requisitos: Python 3.10 o superior y Java 17.

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Descargar los datos (enero y la tabla de zonas para empezar):

```bash
curl -L -o data/yellow_taxi/yellow_tripdata_2025-01.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
curl -L -o data/taxi_zone_lookup.csv https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Ejecutar:

```bash
.venv/bin/python exploracion/explorar_enero.py
.venv/bin/python scripts/main.py --local --consultas q05,q06
```

Los resultados quedan en `resultados/qXX/` como CSV. En VS Code hay dos configuraciones de ejecución ya preparadas (Run and Debug).

## Cómo añadir una consulta

1. Implementar su función en `scripts/consultas.py`. Recibe el DataFrame preparado y devuelve un DataFrame con el resultado agregado.
2. Las columnas disponibles están documentadas al principio de `scripts/preparar_datos.py`.
3. Probar en local: `python scripts/main.py --local --consultas qXX`.

## Ejecución en el clúster

Desde el nodo client:

```bash
hdfs dfs -mkdir -p /datos/yellow_taxi
hdfs dfs -put data/yellow_taxi/*.parquet /datos/yellow_taxi/
hdfs dfs -put data/taxi_zone_lookup.csv /datos/

spark-submit --master yarn \
  --py-files scripts/cargar_datos.py,scripts/preparar_datos.py,scripts/consultas.py \
  scripts/main.py --viajes hdfs:///datos/yellow_taxi \
  --zonas hdfs:///datos/taxi_zone_lookup.csv --salida hdfs:///resultados

hdfs dfs -ls /resultados
```

## Fuentes

- NYC TLC Trip Record Data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Taxi Zone Lookup Table: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
- Meteorología (pendiente): https://www.ncei.noaa.gov/cdo-web/
=======
# proyecto-BigData
>>>>>>> 414b256327a15524654d24a2e0d8f46c4de0dbe8
