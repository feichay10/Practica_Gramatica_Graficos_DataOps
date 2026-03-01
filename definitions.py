from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules

# Importamos los módulos
from scripts import ampliacion, test_checks_assets

defs = Definitions(
    # Cargar los assets desde ampliacion.py
    assets=load_assets_from_modules([ampliacion]),

    # Cargar los checks desde test_checks_assets.py
    asset_checks=load_asset_checks_from_modules([test_checks_assets]),
)