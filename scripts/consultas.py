"""Las 10 consultas analíticas.

Interfaz: cada consulta es una función qXX(viajes) -> DataFrame, donde
`viajes` es el resultado de preparar_datos.preparar(). Para añadir una consulta
basta con implementar su función; main.py la ejecuta y guarda el resultado.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


# ---------- Ane ----------
def q01(viajes: DataFrame) -> DataFrame:
    """¿En qué meses se realizan más viajes?"""
    raise NotImplementedError("Q01 · Ane")


def q02(viajes: DataFrame) -> DataFrame:
    """¿Qué días de la semana registran más actividad?"""
    raise NotImplementedError("Q02 · Ane")


def q07(viajes: DataFrame) -> DataFrame:
    """¿En qué distritos es mayor el importe medio del viaje?"""
    raise NotImplementedError("Q07 · Ane")


# ---------- Xabi ----------
def q03(viajes: DataFrame) -> DataFrame:
    """¿En qué horas del día se inician más viajes?"""
    raise NotImplementedError("Q03 · Xabi")


def q04(viajes: DataFrame) -> DataFrame:
    """¿Qué distritos concentran más viajes iniciados?"""
    raise NotImplementedError("Q04 · Xabi")


# ---------- Ainhoa ----------
def q05(viajes: DataFrame) -> DataFrame:
    """¿Cuáles son las zonas de origen más frecuentes?

    Para cada zona: viajes, % sobre el total de la ciudad, posición en la
    ciudad y posición dentro de su distrito (función de ventana).
    """
    total = viajes.count()
    por_zona = (viajes
                .filter(F.col("pu_zone").isNotNull())
                .groupBy("pu_borough", "pu_zone")
                .agg(F.count("*").alias("viajes"),
                     F.round(F.avg("total_amount"), 2).alias("importe_medio")))
    return (por_zona
            .withColumn("pct_ciudad", F.round(F.col("viajes") * 100.0 / total, 3))
            .withColumn("rank_ciudad", F.rank().over(Window.orderBy(F.desc("viajes"))))
            .withColumn("rank_distrito", F.rank().over(
                Window.partitionBy("pu_borough").orderBy(F.desc("viajes"))))
            .orderBy("rank_ciudad"))


def q06(viajes: DataFrame) -> DataFrame:
    """¿Cuáles son las rutas entre zonas más habituales?

    Para cada par origen-destino: viajes, duración, distancia e importe medios
    y si la ruta se queda dentro de la misma zona.
    """
    return (viajes
            .filter(F.col("pu_zone").isNotNull() & F.col("do_zone").isNotNull())
            .groupBy("pu_borough", "pu_zone", "do_borough", "do_zone")
            .agg(F.count("*").alias("viajes"),
                 F.round(F.avg("duracion_min"), 1).alias("duracion_media_min"),
                 F.round(F.avg("trip_distance"), 2).alias("distancia_media_millas"),
                 F.round(F.avg("total_amount"), 2).alias("importe_medio"))
            .withColumn("misma_zona", F.col("pu_zone") == F.col("do_zone"))
            .withColumn("rank", F.rank().over(Window.orderBy(F.desc("viajes"))))
            .orderBy("rank"))


# ---------- Keltse ----------
def q08(viajes: DataFrame) -> DataFrame:
    """¿En qué franjas horarias duran más los viajes?"""
    raise NotImplementedError("Q08 · Keltse")


def q09(viajes: DataFrame) -> DataFrame:
    """¿Cómo cambia el importe medio según la distancia?"""
    raise NotImplementedError("Q09 · Keltse")


def q10(viajes: DataFrame) -> DataFrame:
    """¿Difiere el volumen diario entre días lluviosos y secos? (necesita meteo)"""
    raise NotImplementedError("Q10 · Keltse")


CONSULTAS = {f.__name__: f for f in (q01, q02, q03, q04, q05, q06, q07, q08, q09, q10)}
