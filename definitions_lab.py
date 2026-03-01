from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
# Supongamos que tu archivo se llama proyecto_islas.py
from scripts import test_checks

defs = Definitions(
    assets=load_assets_from_modules([test_checks]),
    # ¡AQUÍ ESTÁ LA CLAVE! Debes añadir el check aquí:
    asset_checks=load_asset_checks_from_modules([test_checks])
)