"""Etapa 1 · Cuenta los registros de enero afectados por cada regla de limpieza.

Rellena la plantilla de criterios de NOTAS_KELTSE.md. Las reglas no son excluyentes:
un mismo viaje puede incumplir varias.

Uso:
    python scripts/calidad_enero.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

VIAJES = "data/yellow_tripdata_2025-01.parquet"

ZONAS_DESCONOCIDAS = [264, 265]


def duracion_s():
    return (
        F.unix_timestamp(F.col("tpep_dropoff_datetime"))
        - F.unix_timestamp(F.col("tpep_pickup_datetime"))
    )


def reglas() -> dict:
    """Se construyen aquí porque F.col necesita una sesión de Spark activa."""
    pickup = F.col("tpep_pickup_datetime")
    dropoff = F.col("tpep_dropoff_datetime")
    pasajeros = F.col("passenger_count")
    origen_desconocido = F.col("PULocationID").isin(ZONAS_DESCONOCIDAS)
    destino_desconocido = F.col("DOLocationID").isin(ZONAS_DESCONOCIDAS)
    return {
        "1a. Pickup fuera de 2025": F.year(pickup) != 2025,
        "1b. Pickup fuera de enero de 2025": (F.year(pickup) != 2025) | (F.month(pickup) != 1),
        "2. Dropoff <= pickup": dropoff <= pickup,
        "3. Duración > 3 h": duracion_s() > 3 * 3600,
        "4. trip_distance <= 0": F.col("trip_distance") <= 0,
        "5. trip_distance > 100 millas": F.col("trip_distance") > 100,
        "6. total_amount < 0": F.col("total_amount") < 0,
        "7. fare_amount <= 0": F.col("fare_amount") <= 0,
        "8a. passenger_count nulo": pasajeros.isNull(),
        "8b. passenger_count = 0": pasajeros == 0,
        "9a. PULocationID 264-265": origen_desconocido,
        "9b. DOLocationID 264-265": destino_desconocido,
        "9c. Origen o destino 264-265": origen_desconocido | destino_desconocido,
    }


def main() -> None:
    spark = (
        SparkSession.builder.appName("calidad_enero")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    viajes = spark.read.parquet(VIAJES)
    total = viajes.count()

    # Un solo agregado: recorre el fichero una vez en lugar de una por regla
    conteos = viajes.agg(
        *[F.sum(cond.cast("int")).alias(regla) for regla, cond in reglas().items()]
    ).first().asDict()

    print(f"\nTotal de viajes: {total:,}\n")
    for regla, n in conteos.items():
        print(f"{regla:<36} {n:>10,}  ({n / total:7.3%})")

    print("\n¿Los nulos de passenger_count son siempre las mismas filas?")
    pasajeros = F.col("passenger_count")
    columnas = ["RatecodeID", "store_and_fwd_flag", "congestion_surcharge", "Airport_fee"]
    nulos = viajes.filter(pasajeros.isNull())
    for col in columnas:
        print(f"  {col} nulo dentro de ese grupo: {nulos.filter(F.col(col).isNull()).count():,}")

    print("\npayment_type de todo el fichero frente a las filas con passenger_count nulo:")
    viajes.groupBy("payment_type").agg(
        F.count("*").alias("viajes"),
        F.sum(pasajeros.isNull().cast("int")).alias("con_passenger_count_nulo"),
    ).orderBy("payment_type").show()

    print("Rangos para valorar los umbrales:")
    viajes.select(
        F.min("trip_distance").alias("dist_min"),
        F.expr("percentile_approx(trip_distance, 0.5)").alias("dist_p50"),
        F.expr("percentile_approx(trip_distance, 0.999)").alias("dist_p99_9"),
        F.max("trip_distance").alias("dist_max"),
        F.min("total_amount").alias("total_min"),
        F.max("total_amount").alias("total_max"),
        (F.max(duracion_s()) / 3600).alias("duracion_max_h"),
    ).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
