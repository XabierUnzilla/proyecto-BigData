"""Etapa 1 · Aplica las reglas de limpieza a los 12 meses de 2025.

Muestra el total anual de cada regla y su rango mensual, cuántos viajes quedan para
cada grupo de consultas y datos para decidir el límite superior de importe.

Uso:
    python scripts/calidad_anual.py RUTA_A_LA_CARPETA_DE_PARQUET
"""

import argparse
import sys
from functools import reduce
from operator import or_
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from calidad_enero import duracion_s, reglas

UMBRALES_IMPORTE = [200, 500, 1000, 5000]


def imprimir_reglas(viajes, condiciones: dict, total: int) -> None:
    filas = viajes.groupBy(F.input_file_name().alias("fichero")).agg(
        F.count("*").alias("viajes"),
        *[F.sum(c.cast("int")).alias(r) for r, c in condiciones.items()],
    ).collect()

    print(f"\n{'regla':<34}{'viajes':>12}{'% año':>9}{'% mes mín':>11}{'% mes máx':>11}")
    for regla in condiciones:
        n = sum(f[regla] for f in filas)
        pct = [f[regla] / f["viajes"] for f in filas]
        print(f"{regla:<34}{n:>12,}{n / total:>9.3%}{min(pct):>11.3%}{max(pct):>11.3%}")


def imprimir_viajes_validos(viajes, c: dict, total: int) -> None:
    grupos = {
        "Duración (Q08): reglas 1, 2, 3": ["1a. Pickup fuera de 2025", "2. Dropoff <= pickup",
                                           "3. Duración > 3 h"],
        "Importes: reglas 1, 6, 7": ["1a. Pickup fuera de 2025", "6. total_amount < 0",
                                     "7. fare_amount <= 0"],
        "Distancia (Q09): reglas 1, 4, 5, 6, 7": ["1a. Pickup fuera de 2025",
                                                  "4. trip_distance <= 0",
                                                  "5. trip_distance > 100 millas",
                                                  "6. total_amount < 0",
                                                  "7. fare_amount <= 0"],
    }
    fila = viajes.agg(*[
        F.sum(reduce(or_, [c[r] for r in rs]).cast("int")).alias(g) for g, rs in grupos.items()
    ]).first()

    print("\nViajes excluidos por grupo de consultas (sin contar dos veces):")
    for g in grupos:
        print(f"  {g:<40}{fila[g]:>12,}  ({fila[g] / total:.3%})  quedan {total - fila[g]:,}")


def imprimir_importes(viajes) -> None:
    print("\nViajes con fare_amount <= 0, según su forma:")
    viajes.filter(F.col("fare_amount") <= 0).groupBy(
        F.when(F.col("fare_amount") == 0, "fare = 0").otherwise("fare < 0").alias("fare"),
        F.when(F.col("total_amount") < 0, "total < 0")
        .when(F.col("total_amount") == 0, "total = 0")
        .otherwise("total > 0").alias("total"),
        "payment_type",
    ).agg(
        F.count("*").alias("viajes"),
        F.round(F.avg("total_amount"), 2).alias("total_medio"),
        F.round(F.avg("trip_distance"), 2).alias("distancia_media"),
    ).orderBy(F.desc("viajes")).show(30, truncate=False)

    print("Cola alta de total_amount y fare_amount:")
    viajes.select(
        F.expr("percentile_approx(total_amount, array(0.5, 0.99, 0.999, 0.9999))")
        .alias("total_p50_p99_p999_p9999"),
        F.max("total_amount").alias("total_max"),
        F.max("fare_amount").alias("fare_max"),
        F.max("trip_distance").alias("distancia_max"),
        (F.max(duracion_s()) / 3600).alias("duracion_max_h"),
    ).show(truncate=False, vertical=True)

    conteos = viajes.agg(*[
        F.sum((F.col("total_amount") > u).cast("int")).alias(str(u)) for u in UMBRALES_IMPORTE
    ]).first()
    print("Viajes por encima de cada umbral de total_amount:")
    for u in UMBRALES_IMPORTE:
        print(f"  > {u:>5}: {conteos[str(u)]:>8,}")


def imprimir_desglose(viajes, c: dict) -> None:
    """Muestra en qué meses y tipos de pago se concentran las reglas que más varían."""
    regla_2 = c["2. Dropoff <= pickup"]
    regla_7 = c["7. fare_amount <= 0"]
    fare_neg_total_pos = (F.col("fare_amount") < 0) & (F.col("total_amount") > 0)
    mes = F.regexp_extract(F.input_file_name(), r"(\d{4}-\d{2})\.parquet", 1).alias("mes")

    print("\nPor mes del archivo:")
    viajes.groupBy(mes).agg(
        F.count("*").alias("viajes"),
        F.sum(regla_2.cast("int")).alias("dropoff_lte_pickup"),
        F.sum(regla_7.cast("int")).alias("fare_lte_0"),
        F.sum(fare_neg_total_pos.cast("int")).alias("fare_neg_total_pos"),
        F.sum((F.col("payment_type") == 0).cast("int")).alias("payment_0"),
    ).orderBy("mes").show(12, truncate=False)

    print("Por payment_type:")
    viajes.groupBy("payment_type").agg(
        F.count("*").alias("viajes"),
        F.sum(regla_2.cast("int")).alias("dropoff_lte_pickup"),
        F.sum(regla_7.cast("int")).alias("fare_lte_0"),
        F.sum(fare_neg_total_pos.cast("int")).alias("fare_neg_total_pos"),
        F.sum(F.col("passenger_count").isNull().cast("int")).alias("pax_nulo"),
    ).orderBy("payment_type").show(truncate=False)

    print("Por VendorID:")
    viajes.groupBy("VendorID").agg(
        F.count("*").alias("viajes"),
        F.sum(regla_2.cast("int")).alias("dropoff_lte_pickup"),
        F.sum(fare_neg_total_pos.cast("int")).alias("fare_neg_total_pos"),
        F.sum((F.col("payment_type") == 0).cast("int")).alias("payment_0"),
    ).orderBy("VendorID").show(truncate=False)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("carpeta", type=Path, help="carpeta con yellow_tripdata_2025-*.parquet")
    args = parser.parse_args()

    ficheros = sorted(args.carpeta.resolve().glob("yellow_tripdata_2025-*.parquet"))
    if not ficheros:
        sys.exit(f"No hay ficheros yellow_tripdata_2025-*.parquet en {args.carpeta}")

    spark = (
        SparkSession.builder.appName("calidad_anual")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Sin cache(): no cabe en la memoria por defecto y Parquet se relee rápido
    viajes = spark.read.parquet(*[f.as_posix() for f in ficheros])
    total = viajes.count()
    print(f"\nFicheros: {len(ficheros)}  ·  Total de viajes: {total:,}")

    condiciones = reglas()
    # "Fuera de enero" solo tiene sentido para el archivo de enero
    del condiciones["1b. Pickup fuera de enero de 2025"]

    imprimir_reglas(viajes, condiciones, total)
    imprimir_viajes_validos(viajes, condiciones, total)
    imprimir_desglose(viajes, condiciones)
    imprimir_importes(viajes)
    spark.stop()


if __name__ == "__main__":
    main()
