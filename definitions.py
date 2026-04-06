from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules

# Importamos los módulos
from scripts import ampliacion, test_prompt, test_checks_assets
from scripts.sensors import pipeline_rentas_job, sensor_carpeta_datos

defs = Definitions(
    # Cargar los assets desde ampliacion.py y test_prompt.py
    assets=load_assets_from_modules([ampliacion, test_prompt]),

    # Cargar los checks desde test_checks_assets.py
    asset_checks=load_asset_checks_from_modules([test_checks_assets]),

    # Job que agrupa todos los assets (necesario para el sensor)
    jobs=[pipeline_rentas_job],

    # Sensor que vigila la carpeta data/ y lanza el pipeline si hay cambios
    sensors=[sensor_carpeta_datos],
)