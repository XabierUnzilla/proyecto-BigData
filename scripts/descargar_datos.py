"""Descarga los Parquet de Yellow Taxi 2025 y la tabla de zonas en data/.

Uso:
    python scripts/descargar_datos.py            # solo enero + zonas
    python scripts/descargar_datos.py --todos    # los 12 meses + zonas

Los ficheros que ya existen no se vuelven a descargar.
"""

import argparse
import sys
import urllib.request
from pathlib import Path

BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
ANIO = 2025


def descargar(url: str, destino: Path) -> None:
    if destino.exists():
        print(f"[ya existe] {destino.name}")
        return
    print(f"[descargando] {url}")
    tmp = destino.with_suffix(destino.suffix + ".part")
    try:
        with urllib.request.urlopen(url) as resp, open(tmp, "wb") as f:
            while bloque := resp.read(1 << 20):
                f.write(bloque)
    except Exception as e:
        tmp.unlink(missing_ok=True)
        print(f"[error] {destino.name}: {e}", file=sys.stderr)
        return
    tmp.rename(destino)
    print(f"[ok] {destino.name} ({destino.stat().st_size / 1e6:.1f} MB)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--todos", action="store_true", help="descargar los 12 meses")
    args = parser.parse_args()

    DATA_DIR.mkdir(exist_ok=True)
    descargar(f"{BASE_URL}/misc/taxi_zone_lookup.csv", DATA_DIR / "taxi_zone_lookup.csv")

    meses = range(1, 13) if args.todos else [1]
    for mes in meses:
        nombre = f"yellow_tripdata_{ANIO}-{mes:02d}.parquet"
        descargar(f"{BASE_URL}/trip-data/{nombre}", DATA_DIR / nombre)


if __name__ == "__main__":
    main()
