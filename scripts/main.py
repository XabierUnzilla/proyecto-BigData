"""Ejecuta las consultas y guarda cada resultado en <salida>/qXX/.

En local:
    python scripts/main.py --local --consultas q05,q06

En el clúster (desde el nodo client):
    spark-submit --master yarn --py-files scripts/cargar_datos.py,scripts/preparar_datos.py,scripts/consultas.py \\
        scripts/main.py --viajes hdfs:///datos/yellow_taxi --zonas hdfs:///datos/taxi_zone_lookup.csv \\
        --salida hdfs:///resultados
"""
import argparse
import time

from cargar_datos import crear_spark
from consultas import CONSULTAS
from preparar_datos import preparar


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--viajes", default="data/yellow_taxi")
    p.add_argument("--zonas", default="data/taxi_zone_lookup.csv")
    p.add_argument("--salida", default="resultados")
    p.add_argument("--consultas", default="all", help="p. ej. q05,q06 o all")
    p.add_argument("--local", action="store_true", help="Spark en local[*]")
    p.add_argument("--mostrar", type=int, default=10, help="filas a mostrar por consola")
    args = p.parse_args()

    spark = crear_spark(local=args.local)
    viajes = preparar(spark, args.viajes, args.zonas).cache()
    print(f"Viajes tras la limpieza: {viajes.count():,}")

    nombres = list(CONSULTAS) if args.consultas == "all" else args.consultas.split(",")
    for nombre in nombres:
        t0 = time.time()
        try:
            resultado = CONSULTAS[nombre](viajes)
        except NotImplementedError as e:
            print(f"[{nombre}] pendiente: {e}")
            continue
        destino = f"{args.salida}/{nombre}"
        # Los resultados son tablas agregadas pequeñas: un único CSV por consulta.
        resultado.coalesce(1).write.mode("overwrite").option("header", True).csv(destino)
        print(f"\n[{nombre}] guardado en {destino} ({time.time() - t0:.1f} s)")
        resultado.show(args.mostrar, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
