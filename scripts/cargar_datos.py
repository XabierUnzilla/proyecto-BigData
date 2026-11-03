"""Lectura de los datos de entrada con Spark DataFrame.

Las rutas son parámetros: el mismo código sirve en local (data/...) y en el
clúster (hdfs:///...).
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Esquema común de los viajes. Los Parquet mensuales de TLC no siempre traen los
# mismos tipos (p. ej. passenger_count int64 o double) ni las mismas mayúsculas
# (Airport_fee / airport_fee), así que cada fichero se normaliza a este esquema.
COLUMNAS_VIAJES = {
    "vendorid": "int",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "int",
    "trip_distance": "double",
    "ratecodeid": "int",
    "pulocationid": "int",
    "dolocationid": "int",
    "payment_type": "int",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double",
    "cbd_congestion_fee": "double",
}


def crear_spark(nombre: str = "ATBD-taxis", local: bool = False) -> SparkSession:
    """Crea la sesión de Spark. En el clúster el master lo pone spark-submit."""
    builder = SparkSession.builder.appName(nombre)
    if local:
        builder = (builder.master("local[*]")
                   .config("spark.driver.memory", "4g")
                   .config("spark.sql.shuffle.partitions", "8"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _listar_parquet(spark: SparkSession, ruta: str) -> list[str]:
    """Devuelve los .parquet de una carpeta (local o HDFS) o la propia ruta si es un fichero."""
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(ruta)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if fs.isFile(path):
        return [ruta]
    return sorted(str(s.getPath()) for s in fs.listStatus(path)
                  if s.getPath().getName().endswith(".parquet"))


def _normalizar(df: DataFrame) -> DataFrame:
    df = df.toDF(*[c.lower() for c in df.columns])
    return df.select([
        (F.col(c) if c in df.columns else F.lit(None)).cast(t).alias(c)
        for c, t in COLUMNAS_VIAJES.items()
    ])


def cargar_viajes(spark: SparkSession, ruta: str) -> DataFrame:
    """Lee uno o varios Parquet mensuales como una única tabla de viajes."""
    ficheros = _listar_parquet(spark, ruta)
    if not ficheros:
        raise FileNotFoundError(f"No hay ficheros .parquet en {ruta}")
    df = None
    for f in ficheros:
        mes = _normalizar(spark.read.parquet(f))
        df = mes if df is None else df.unionByName(mes)
    return df


def cargar_zonas(spark: SparkSession, ruta: str) -> DataFrame:
    """Lee taxi_zone_lookup.csv (LocationID, Borough, Zone, service_zone)."""
    return (spark.read.option("header", True).csv(ruta)
            .select(F.col("LocationID").cast("int").alias("location_id"),
                    F.col("Borough").alias("borough"),
                    F.col("Zone").alias("zone"),
                    F.col("service_zone")))
