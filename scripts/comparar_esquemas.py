"""Etapa 1 · Comprueba que los 12 Parquet de 2025 tienen un esquema compatible.

Compara cada mes con el primero (columnas, tipos y mayúsculas) y después lee todos
los ficheros a la vez para confirmar que Spark puede tratarlos como una sola tabla.

Uso:
    python scripts/comparar_esquemas.py RUTA_A_LA_CARPETA_DE_PARQUET
"""

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def comparar_esquemas(spark: SparkSession, ficheros: list[Path]) -> int:
    """Imprime las diferencias con el primer fichero y devuelve cuántas ha encontrado."""
    esquemas = {
        f.name: {c.name: c.dataType.simpleString() for c in spark.read.parquet(f.as_posix()).schema}
        for f in ficheros
    }
    nombre_ref = ficheros[0].name
    ref = esquemas[nombre_ref]
    print(f"Referencia: {nombre_ref} ({len(ref)} columnas)")

    diferencias = 0
    for nombre, esquema in esquemas.items():
        cambios = []
        for col in sorted(ref.keys() - esquema.keys()):
            cambios.append(f"falta {col}")
        for col in sorted(esquema.keys() - ref.keys()):
            cambios.append(f"columna nueva {col} ({esquema[col]})")
        for col in sorted(ref.keys() & esquema.keys()):
            if ref[col] != esquema[col]:
                cambios.append(f"{col}: {ref[col]} -> {esquema[col]}")
        diferencias += len(cambios)
        print(f"  {nombre}: {'igual' if not cambios else '; '.join(cambios)}")
    return diferencias


def comprobar_lectura_conjunta(spark: SparkSession, ficheros: list[Path]) -> None:
    """Lee todas las columnas de los 12 ficheros: los tipos incompatibles fallan aquí."""
    viajes = spark.read.parquet(*[f.as_posix() for f in ficheros])
    agregados = viajes.select(
        F.input_file_name().alias("fichero"),
        F.date_format("tpep_pickup_datetime", "yyyy-MM").alias("mes_pickup"),
        *[F.col(c).isNull().cast("int").alias(c) for c in viajes.columns],
    )
    filas = agregados.groupBy("fichero").agg(
        F.count("*").alias("viajes"),
        F.sum((F.substring("mes_pickup", 1, 4) != "2025").cast("int")).alias("fuera_2025"),
        F.sum((F.col("mes_pickup") != F.regexp_extract("fichero", r"(\d{4}-\d{2})\.parquet", 1))
              .cast("int")).alias("fuera_del_mes"),
        F.sum("passenger_count").alias("passenger_count_nulo"),
        F.sum("Airport_fee").alias("Airport_fee_nulo"),
        F.sum("cbd_congestion_fee").alias("cbd_congestion_fee_nulo"),
    ).orderBy("fichero").collect()

    print(f"\n{'mes':<8}{'viajes':>12}{'fuera 2025':>12}{'fuera mes':>11}"
          f"{'pax nulo':>11}{'airport nulo':>14}{'cbd nulo':>10}")
    for f in filas:
        mes = f["fichero"].rsplit("_", 1)[-1].removesuffix(".parquet")
        print(f"{mes:<8}{f['viajes']:>12,}{f['fuera_2025']:>12,}{f['fuera_del_mes']:>11,}"
              f"{f['passenger_count_nulo']:>11,}{f['Airport_fee_nulo']:>14,}"
              f"{f['cbd_congestion_fee_nulo']:>10,}")
    print(f"{'TOTAL':<8}{sum(f['viajes'] for f in filas):>12,}"
          f"{sum(f['fuera_2025'] for f in filas):>12,}"
          f"{sum(f['fuera_del_mes'] for f in filas):>11,}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("carpeta", type=Path, help="carpeta con yellow_tripdata_2025-*.parquet")
    args = parser.parse_args()

    ficheros = sorted(args.carpeta.resolve().glob("yellow_tripdata_2025-*.parquet"))
    if not ficheros:
        sys.exit(f"No hay ficheros yellow_tripdata_2025-*.parquet en {args.carpeta}")
    print(f"Ficheros encontrados: {len(ficheros)}\n")

    spark = (
        SparkSession.builder.appName("comparar_esquemas")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    diferencias = comparar_esquemas(spark, ficheros)
    print(f"\nDiferencias de esquema: {diferencias}")

    comprobar_lectura_conjunta(spark, ficheros)
    spark.stop()


if __name__ == "__main__":
    main()
