"""
Sensor de Dagster que vigila la carpeta data/ del proyecto.
Si se añade un fichero nuevo o cambia alguno existente, dispara
el pipeline completo (pipeline_rentas_job).
"""

import os
import json
from pathlib import Path

from dagster import (
    sensor,
    RunRequest,
    AssetSelection,
    define_asset_job,
    DefaultSensorStatus,
)

# ── Ruta a la carpeta de datos ────────────────────────────────────────────────
_DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# ── Job: ejecuta todos los assets del pipeline ────────────────────────────────
pipeline_rentas_job = define_asset_job(
    name="pipeline_rentas_job",
    selection=AssetSelection.all(),
)


# ── Sensor: vigila cambios en data/ ──────────────────────────────────────────
@sensor(
    job=pipeline_rentas_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=30,
)
def sensor_carpeta_datos(context):
    """
    Comprueba periódicamente los ficheros de la carpeta data/.
    Si detecta que algún fichero ha cambiado su fecha de modificación
    o ha aparecido un fichero nuevo, lanza el pipeline completo.

    El cursor almacena un JSON con { ruta: mtime } de todos los ficheros
    conocidos en la última ejecución del sensor.
    """
    # Leer estado anterior desde el cursor
    cursor_raw = context.cursor
    estado_anterior = json.loads(cursor_raw) if cursor_raw else {}

    # Calcular estado actual de la carpeta data/
    estado_actual = {}
    if _DATA_DIR.exists():
        for fichero in sorted(_DATA_DIR.iterdir()):
            if fichero.is_file():
                estado_actual[str(fichero.name)] = str(os.path.getmtime(fichero))

    # Detectar cambios: ficheros nuevos o modificados
    cambios = []
    for nombre, mtime in estado_actual.items():
        if nombre not in estado_anterior:
            cambios.append(f"Nuevo fichero: {nombre}")
        elif estado_anterior[nombre] != mtime:
            cambios.append(f"Fichero modificado: {nombre}")

    if cambios:
        context.log.info(f"Cambios detectados en data/: {cambios}")
        # Usamos la firma del estado actual como run_key para evitar duplicados
        run_key = json.dumps(estado_actual, sort_keys=True)
        context.update_cursor(json.dumps(estado_actual))
        yield RunRequest(
            run_key=run_key,
            tags={"trigger": "sensor_carpeta_datos", "cambios": str(cambios)},
        )
    else:
        context.log.debug("Sin cambios en data/. Pipeline no lanzado.")
        # Actualizamos el cursor aunque no haya cambios (primera ejecución)
        if not cursor_raw:
            context.update_cursor(json.dumps(estado_actual))