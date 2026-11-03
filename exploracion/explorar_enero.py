"""Etapa 1: comprobaciones sobre enero de 2025 (apoyo para Keltse).

    python exploracion/explorar_enero.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from pyspark.sql import functions as F  # noqa: E402

from cargar_datos import cargar_viajes, cargar_zonas, crear_spark  # noqa: E402
from preparar_datos import anadir_campos, limpiar  # noqa: E402

spark = crear_spark("explorar-enero", local=True)
viajes = cargar_viajes(spark, "data/yellow_taxi/yellow_tripdata_2025-01.parquet").cache()
zonas = cargar_zonas(spark, "data/taxi_zone_lookup.csv")

print("\n== Esquema normalizado ==")
viajes.printSchema()
viajes.show(5, truncate=False)

total = viajes.count()
print(f"\n== Registros: {total:,} ==")

print("\n== Nulos por columna ==")
viajes.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in viajes.columns]) \
      .show(vertical=True)

print("\n== Tabla de zonas ==")
n_zonas, n_ids = zonas.count(), zonas.select("location_id").distinct().count()
print(f"Filas: {n_zonas} · LocationID distintos: {n_ids} · ¿únicos?: {n_zonas == n_ids}")

print("\n== IDs de viajes que no están en la tabla de zonas ==")
ids_zonas = zonas.select(F.col("location_id").alias("id"))
for col in ("pulocationid", "dolocationid"):
    huerfanos = viajes.join(ids_zonas, viajes[col] == ids_zonas.id, "left_anti")
    print(f"{col}: {huerfanos.count():,} viajes sin zona")

print("\n== Zonas 'Unknown' / fuera de NYC (264, 265) usadas como origen ==")
viajes.filter(F.col("pulocationid").isin(264, 265)).groupBy("pulocationid").count().show()

print("\n== Rangos sospechosos ==")
c = anadir_campos(viajes)
checks = {
    "fecha fuera de enero 2025": ~((F.col("anio") == 2025) & (F.col("mes") == 1)),
    "fin <= inicio": F.col("duracion_min") <= 0,
    "duración > 4 h": F.col("duracion_min") > 240,
    "distancia <= 0": F.col("trip_distance") <= 0,
    "distancia > 100 millas": F.col("trip_distance") > 100,
    "fare_amount < 0": F.col("fare_amount") < 0,
    "total_amount <= 0": F.col("total_amount") <= 0,
    "passenger_count nulo o 0": F.col("passenger_count").isNull() | (F.col("passenger_count") == 0),
}
c.select([F.sum(cond.cast("int")).alias(n) for n, cond in checks.items()]).show(vertical=True)

print("\n== Estadísticos ==")
c.select("trip_distance", "duracion_min", "fare_amount", "total_amount", "tip_amount") \
 .summary("min", "25%", "50%", "75%", "max", "mean").show()

limpios = limpiar(c).count()
print(f"\n== Tras la limpieza provisional: {limpios:,} de {total:,} "
      f"({limpios / total:.1%}) ==")
spark.stop()
