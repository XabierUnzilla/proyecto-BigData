"""Limpieza, columnas derivadas y join con la tabla de zonas.

Interfaz común para todas las consultas: preparar() devuelve un DataFrame con
las columnas originales normalizadas (ver cargar_datos.COLUMNAS_VIAJES) más:

    fecha, anio, mes, dia_semana (1=domingo ... 7=sábado), hora, duracion_min
    pu_borough, pu_zone, pu_service_zone   (zona de origen)
    do_borough, do_zone, do_service_zone   (zona de destino)
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cargar_datos import cargar_viajes, cargar_zonas

ANIO = 2025


def anadir_campos(df: DataFrame) -> DataFrame:
    inicio, fin = F.col("tpep_pickup_datetime"), F.col("tpep_dropoff_datetime")
    return (df
            .withColumn("fecha", F.to_date(inicio))
            .withColumn("anio", F.year(inicio))
            .withColumn("mes", F.month(inicio))
            .withColumn("dia_semana", F.dayofweek(inicio))
            .withColumn("hora", F.hour(inicio))
            .withColumn("duracion_min",
                        (F.unix_timestamp(fin) - F.unix_timestamp(inicio)) / 60.0))


def limpiar(df: DataFrame) -> DataFrame:
    """Filtros de calidad.

    PROVISIONAL: los criterios definitivos los cierra Keltse (etapa 1) y se
    revisan el domingo 8. Cambiar solo aquí para que todas las consultas usen
    el mismo conjunto de viajes.
    """
    return df.filter(
        (F.col("anio") == ANIO)
        & (F.col("duracion_min") >= 1) & (F.col("duracion_min") <= 240)
        & (F.col("trip_distance") > 0) & (F.col("trip_distance") <= 100)
        & (F.col("fare_amount") >= 0)
        & (F.col("total_amount") > 0)
    )


def unir_zonas(df: DataFrame, zonas: DataFrame) -> DataFrame:
    """Une la tabla de zonas dos veces: como origen y como destino.

    Left join: un viaje con un ID sin zona no se pierde, queda con nulos.
    """
    origen = zonas.select(F.col("location_id").alias("pulocationid"),
                          F.col("borough").alias("pu_borough"),
                          F.col("zone").alias("pu_zone"),
                          F.col("service_zone").alias("pu_service_zone"))
    destino = zonas.select(F.col("location_id").alias("dolocationid"),
                           F.col("borough").alias("do_borough"),
                           F.col("zone").alias("do_zone"),
                           F.col("service_zone").alias("do_service_zone"))
    # La tabla de zonas es pequeña (265 filas): broadcast evita el shuffle.
    return (df.join(F.broadcast(origen), "pulocationid", "left")
              .join(F.broadcast(destino), "dolocationid", "left"))


def preparar(spark: SparkSession, ruta_viajes: str, ruta_zonas: str) -> DataFrame:
    viajes = cargar_viajes(spark, ruta_viajes)
    zonas = cargar_zonas(spark, ruta_zonas)
    return unir_zonas(limpiar(anadir_campos(viajes)), zonas)
