"""Etapa 1 · Exploración de enero de 2025 (Yellow Taxi NYC).

Muestra esquema, 5 filas, recuento, nulos por columna, rango de fechas,
unicidad de LocationID y cruce PULocationID/DOLocationID con la tabla de zonas.

Uso:
    python scripts/explorar_enero.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

VIAJES = "data/yellow_tripdata_2025-01.parquet"
ZONAS = "data/taxi_zone_lookup.csv"


def titulo(texto: str) -> None:
    print(f"\n{'=' * 70}\n{texto}\n{'=' * 70}")


def main() -> None:
    spark = (
        SparkSession.builder.appName("explorar_enero")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    viajes = spark.read.parquet(VIAJES)
    zonas = spark.read.csv(ZONAS, header=True, inferSchema=True)

    titulo("1. Esquema de viajes")
    viajes.printSchema()

    titulo("2. Cinco filas de ejemplo")
    viajes.show(5, truncate=False, vertical=True)

    titulo("3. Recuento total")
    total = viajes.count()
    print(f"Viajes en el fichero: {total:,}")

    titulo("4. Nulos por columna")
    nulos = viajes.select(
        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in viajes.columns]
    ).first().asDict()
    for col, n in nulos.items():
        print(f"{col:<25} {n:>12,}  ({n / total:6.2%})")

    titulo("5. Rango de fechas de recogida")
    viajes.select(
        F.min("tpep_pickup_datetime").alias("min_pickup"),
        F.max("tpep_pickup_datetime").alias("max_pickup"),
    ).show(truncate=False)
    fuera_de_mes = viajes.filter(
        (F.year("tpep_pickup_datetime") != 2025) | (F.month("tpep_pickup_datetime") != 1)
    ).count()
    print(f"Viajes con pickup fuera de enero de 2025: {fuera_de_mes:,}")

    titulo("6. Tabla de zonas")
    zonas.printSchema()
    zonas.show(5, truncate=False)
    n_zonas = zonas.count()
    n_ids = zonas.select("LocationID").distinct().count()
    print(f"Filas: {n_zonas}  ·  LocationID distintos: {n_ids}  ·  "
          f"{'únicos' if n_zonas == n_ids else 'HAY DUPLICADOS'}")
    zonas.filter(F.col("LocationID").isin(264, 265)).show(truncate=False)

    titulo("7. Cruce de PULocationID / DOLocationID con la tabla de zonas")
    ids_zona = zonas.select(F.col("LocationID").alias("id"))
    for col in ("PULocationID", "DOLocationID"):
        sin_zona = viajes.join(ids_zona, viajes[col] == ids_zona["id"], "left_anti")
        n = sin_zona.count()
        print(f"{col}: {n:,} viajes sin zona correspondiente")
        if n:
            sin_zona.groupBy(col).count().orderBy(F.desc("count")).show(10)

    # El join no debe multiplicar filas (solo pasaría si LocationID estuviera duplicado)
    n_join = viajes.join(ids_zona, viajes["PULocationID"] == ids_zona["id"], "left").count()
    print(f"Filas tras left join por PULocationID: {n_join:,} (originales: {total:,})")

    spark.stop()


if __name__ == "__main__":
    main()
