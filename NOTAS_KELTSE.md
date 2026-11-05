# Notas de Keltse · Proyecto ATBD Taxis NYC 2025

Rol: **A · Gestión de datos** (descarga, calidad, limpieza y meteorología) + consultas **Q08, Q09 y Q10**.
Pareja para pasar del local al clúster: **Ainhoa**.

---

## Calendario de mis tareas

| Día | Etapa | Tarea | Hecho |
|---|---|---|---|
| Mar 3 | E1 | Descargar `yellow_tripdata_2025-01.parquet` y `taxi_zone_lookup.csv`; leer el diccionario oficial | ☐ |
| Mié 4 | E1 | Abrir enero con Spark: esquema, 5 filas, recuento y nulos (`explorar_enero.py`) | ☐ |
| Jue 5 | E1 | Comprobar que `LocationID` es único y que `PULocationID`/`DOLocationID` cruzan con el CSV | ☐ |
| Vie 6 | E1 | Descargar los otros 11 Parquet y comparar esquemas (`comparar_esquemas()`) | ☐ |
| Sáb 7 | E2 | NOAA: elegir estación (Central Park), pedir datos de 2025 y revisar unidades | ☐ |
| Sáb 7 | — | Documentar criterios de limpieza (plantilla abajo) | ☐ |
| Dom 8 | Hito | Revisión en grupo: cerrar las 10 preguntas y los criterios de limpieza | ☐ |
| Lun 9 | E2 | Tabla diaria de meteorología: fecha, precipitación, temperatura; 365 días y ausencias | ☐ |
| Lun 9 | — | Decidir con el grupo: meteo válida para Q10 o plan B | ☐ |
| Mar 10 | — | **Q09** · importe medio por tramos de distancia (local) | ☐ |
| Mié 11 | — | **Q08** · duración media por franja horaria, solo viajes coherentes | ☐ |
| Jue 12 | — | **Q10** · viajes diarios + meteorología: lluviosos frente a secos | ☐ |
| Jue 12 | — | Sesión en pareja con Ainhoa (1 h) | ☐ |
| Vie 13 | E6 | Lanzar Q08, Q09 y Q10 en el clúster con `spark-submit` (con Ainhoa) | ☐ |
| Sáb 14 | — | Revisión de código cruzada | ☐ |
| Mar 17 | E7 | Revisar límites y anomalías (lluvia y viajes ≠ causalidad) | ☐ |
| Mié 18 | E8 | Repasar el apartado de datos, limpieza y fuentes del documento | ☐ |
| Jue 19 | — | Ensayo cruzado: explicar la consulta de otra persona y lanzar un trabajo desde cero | ☐ |

> **Criterio de avance del equipo:** nadie programa consultas hasta que enero esté entendido y el cruce `PULocationID ↔ LocationID` funcione (etapa 1 cerrada el día 8). Esto depende de mí.

---

## Entorno en VS Code

```bash
python -m venv .venv
# Windows:   .venv\Scripts\activate
# Mac/Linux: source .venv/bin/activate
pip install pyspark
java -version      # debe ser 17; si no, instalar JDK 17 y configurar JAVA_HOME
python scripts/explorar_enero.py
```

- Los datos van en `data/` (no subir los Parquet a git: añadir `data/` al `.gitignore`).
- En `explorar_enero.py`, cambiar las rutas a `data/yellow_tripdata_2025-01.parquet` y `data/taxi_zone_lookup.csv`.

---

## Fuentes

- Datos de taxi: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Diccionario Yellow Taxi: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
- Tabla de zonas: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
- Meteorología NOAA (Climate Data Online): https://www.ncei.noaa.gov/cdo-web/

---

## Etapa 1 · Qué mirar en el diccionario

- `payment_type`, `RatecodeID`, `VendorID`: son códigos numéricos; anotar qué significa cada valor.
- Cargos: `congestion_surcharge`, `Airport_fee` y **`cbd_congestion_fee`** (nueva en 2025: cargo de la zona de congestión de la MTA desde el 5 de enero de 2025). Puede servir para proponer preguntas menos típicas.
- Fechas: `tpep_pickup_datetime` y `tpep_dropoff_datetime`.
- `trip_distance` está en **millas**.

## Avisos para la exploración

- En la tabla de zonas, los **IDs 264 y 265** son "Unknown" / "Outside of NYC" → decidir si se excluyen en las consultas por zona o distrito.
- El archivo de enero puede traer viajes con fechas de otros meses o años raros → filtrar por fecha de inicio.
- Algún mes puede tener columnas con **distinto tipo** (p. ej. `passenger_count` entero vs. decimal) o nombres distintos (`Airport_fee` / `airport_fee`). Si pasa, leer los 12 Parquet de golpe falla → comparar uno a uno con `comparar_esquemas()` y documentarlo para Ainhoa. **Comprobado (5 nov):** los 12 meses de 2025 tienen las mismas 20 columnas y tipos, y se leen juntos sin error (48.722.602 viajes).
- Comprobar que `LocationID` no tiene duplicados; si los tuviera, el join multiplicaría filas.

---

## Plantilla · Criterios de limpieza (para Ainhoa / `preparar_datos.py`)

Números de `scripts/calidad_enero.py`, sobre los 3.475.226 viajes de enero. Las reglas no son excluyentes: un viaje puede incumplir varias.

| # | Regla | Registros afectados (enero) | % | Decisión | Aplica a |
|---|---|---|---|---|---|
| 1 | Pickup fuera de 2025 (fuera del mes del archivo: 22) | 21 | 0,001 % | Excluir | Todas |
| 2 | Dropoff ≤ pickup | 2.051 | 0,059 % | Excluir | Q08 (duración) |
| 3 | Duración > 3 h | 1.377 | 0,040 % | Excluir | Q08 |
| 4 | `trip_distance` ≤ 0 | 90.893 | 2,615 % | Excluir | Q09 |
| 5 | `trip_distance` > 100 millas | 162 | 0,005 % | Excluir | Q09 |
| 6 | `total_amount` < 0 | 63.037 | 1,814 % | Excluir | Consultas de importe |
| 7 | `fare_amount` ≤ 0 | 145.516 | 4,187 % | Excluir | Consultas de importe |
| 8 | `passenger_count` nulo / 0 | 540.149 / 24.656 | 15,543 % / 0,709 % | Mantener (no se usa en conteos) | — |
| 9 | ID de zona sin correspondencia / 264–265 | Sin correspondencia: 0. Origen 264–265: 9.521. Origen o destino 264–265: 28.357 | 0 % / 0,274 % / 0,816 % | Excluir en consultas por zona | Q04–Q07 |

Idea clave: **no todas las reglas se aplican a todas las consultas**. Un viaje con distancia 0 sigue contando como viaje en Q01, pero no sirve para Q09.

### Para revisar en grupo (día 8)

- **Falta un límite superior de importe:** el máximo de `total_amount` es 863.380,37. Con solo las reglas 6 y 7, ese viaje sigue dentro y puede distorsionar las medias de Q07 y Q09. En el año, el percentil 99,9 ronda los 178 y hay 1.210 viajes por encima de 500, 85 por encima de 1.000 y 6 por encima de 5.000. Propuesta: excluir `total_amount` > 500 en las consultas de importe.
- **Regla 1:** usar "fuera de 2025", no "fuera del mes". Un viaje del 1 de febrero en el archivo de enero sigue siendo válido al leer el año completo. En los 12 archivos, 29 viajes están fuera de 2025 y 214 fuera del mes de su archivo.
- **Regla 7:** en el año excluye el 5,891 %, pero varía mucho por mes (9,5 % en noviembre, 1,2 % en diciembre). De sus 2.870.234 viajes, 1.879.872 tienen tarifa negativa y total positivo, y casi todos son `VendorID` 2 con `payment_type = 0`. Ese patrón pasa de 268.668 viajes en noviembre a 18 en diciembre, y aún no sabemos por qué. Con la regla 7 aplicada, la 6 añade como mucho 29 viajes.
- **Regla 2:** en el año afecta al 1,121 % (546.304), frente al 0,059 % de enero. El 98 % son los 535.901 viajes de `VendorID` 7: todos sus viajes tienen dropoff ≤ pickup. Q08 se queda sin ese proveedor, y hay que documentarlo.
- **Regla 8:** los 540.149 nulos son exactamente los viajes con `payment_type = 0`, y en ellos también son nulos `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge` y `Airport_fee`. Excluirlos quitaría el 15,5 % de los viajes de enero y el 23,8 % del año (11.611.894 de 48.722.602; entre el 15,5 % de enero y el 28,1 % de junio). Falta confirmar en el diccionario qué significa ese código.
- **Regla 9:** Q04, Q05 y Q07 solo usan el origen (9.521 viajes en enero, 103.043 en el año); Q06 usa origen y destino (28.357 en enero, 371.657 en el año).

### Año completo (`scripts/calidad_anual.py`, 48.722.602 viajes)

| Regla | Viajes | % año | % mes mín–máx |
|---|---|---|---|
| 1. Pickup fuera de 2025 | 29 | 0,000 % | 0,000–0,001 % |
| 2. Dropoff ≤ pickup | 546.304 | 1,121 % | 0,059–1,586 % |
| 3. Duración > 3 h | 18.401 | 0,038 % | 0,031–0,050 % |
| 4. `trip_distance` ≤ 0 | 1.402.958 | 2,879 % | 2,303–3,546 % |
| 5. `trip_distance` > 100 millas | 2.870 | 0,006 % | 0,005–0,008 % |
| 6. `total_amount` < 0 | 973.721 | 1,998 % | 1,135–3,035 % |
| 7. `fare_amount` ≤ 0 | 2.870.234 | 5,891 % | 1,178–9,508 % |
| 8. `passenger_count` nulo | 11.611.894 | 23,833 % | 15,543–28,058 % |
| 8. `passenger_count` = 0 | 260.062 | 0,534 % | 0,437–0,709 % |
| 9. Origen 264–265 | 103.043 | 0,211 % | 0,177–0,274 % |
| 9. Origen o destino 264–265 | 371.657 | 0,763 % | 0,685–0,898 % |

Viajes que quedan por grupo de consultas, con las reglas actuales:

| Grupo | Reglas | Excluidos | Quedan |
|---|---|---|---|
| Duración (Q08) | 1, 2, 3 | 564.734 (1,159 %) | 48.157.868 |
| Importes | 1, 6, 7 | 2.870.263 (5,891 %) | 45.852.339 |
| Distancia (Q09) | 1, 4, 5, 6, 7 | 4.022.547 (8,256 %) | 44.700.055 |

---

## Etapa 2 · Meteorología NOAA

1. Climate Data Online → *Search Tool* → **Daily Summaries**.
2. Fechas: 2025-01-01 a 2025-12-31. Estación: **NY CITY CENTRAL PARK, NY US**.
3. Formato CSV, **unidades métricas** (si no, vienen en pulgadas y °F).
4. Variables: **PRCP** (precipitación), **TMAX**, **TMIN** (opcional: SNOW).
5. El pedido llega por correo y puede tardar → **pedirlo cuanto antes**.

Comprobaciones al recibirlo:
- ¿Hay 365 filas? ¿Qué días faltan?
- ¿Nulos en PRCP?
- Formato de `DATE` compatible con `to_date(tpep_pickup_datetime)`.
- Definir "día lluvioso": p. ej. PRCP ≥ 1 mm (documentar el umbral).

> **Plan B:** si el lunes 9 la meteo no está validada, Q10 se sustituye por otra consulta solo con taxis y se documenta la incidencia.

---

## Mis consultas (borrador)

### Q08 · Duración media por franja horaria
- Duración = `(unix_timestamp(dropoff) - unix_timestamp(pickup)) / 60` minutos.
- Solo viajes coherentes (reglas 1–3).
- Agrupar por `hour(tpep_pickup_datetime)` (o franjas: madrugada, mañana, mediodía, tarde, noche).
- Resultado: franja, nº viajes, duración media, mediana (`percentile_approx`).

### Q09 · Importe medio por tramos de distancia
- Tramos con `when`: 0–1, 1–3, 3–5, 5–10, 10–20, > 20 millas.
- Reglas 4–7.
- Resultado: tramo, nº viajes, importe medio, importe medio por milla.

### Q10 · Volumen diario: días lluviosos frente a secos
1. Agregar viajes por `to_date(tpep_pickup_datetime)` → una fila por día.
2. Join con la tabla meteo por fecha.
3. Agrupar por lluvioso/seco (y mejor también por día de la semana, porque el día de la semana influye mucho).
- Resultado: tipo de día, nº de días, media de viajes diarios.
- **Ojo:** asociación ≠ causalidad; una sola estación no representa toda la ciudad.

---

## Recordatorios

- La nota de la defensa es **individual**: tengo que saber lanzar un `spark-submit`, moverme por HDFS (`hdfs dfs -ls`, `-put`, `-cat`) y explicar cualquier consulta, no solo las mías.
- AWS Academy: **parar** las instancias, nunca terminarlas (se pierde HDFS).
- Resultados en HDFS: `hdfs:///resultados/q08/`, `q09/`, `q10/`.

## Pendiente del grupo (no depende de mí, pero afecta)

- Confirmación del profesor: grupo de 4.
- Fase 1 del enunciado: documento de dataset + 10 analíticas en ALUD el **25 nov, 08:00** (no aparece en el calendario).
- Visualización: confirmar por escrito que se ha quitado (la plantilla de resultados aún la pide).
