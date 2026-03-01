import os
import pandas as pd
from dagster import asset_check, AssetCheckResult, AssetIn, MetadataValue

from scripts.ampliacion import (
    raw_codislas,
    renta_municipios,
    grafico_renta_municipios,
)


# ── CHECK 1: CARGA (raw) ──────────────────────────────────────────────────────

@asset_check(asset=raw_codislas)
def check_estandarizacion_islas(raw_codislas):
    """
    Verifica que los nombres de isla están estandarizados.
    Nombres inconsistentes (ej. 'tenerife' vs 'Tenerife') harían que
    plotnine creara categorías duplicadas en la leyenda del gráfico.
    """
    originales   = raw_codislas["ISLA"].nunique()
    normalizadas = raw_codislas["ISLA"].str.capitalize().nunique()
    passed = originales == normalizadas

    return AssetCheckResult(
        passed=passed,
        metadata={
            "categorias_detectadas": MetadataValue.int(originales),
            "categorias_esperadas":  MetadataValue.int(normalizadas),
            "principio_gestalt": MetadataValue.text("Similitud – Evitar fragmentación visual"),
            "mensaje": MetadataValue.text(
                "Si hay nombres inconsistentes, plotnine creará leyendas duplicadas."
            ),
        },
    )


# ── CHECK 2: TRANSFORMACIÓN (curated) ────────────────────────────────────────

@asset_check(asset=renta_municipios)
def check_obs_value_rango(renta_municipios):
    """
    Verifica que OBS_VALUE está entre 0 y 100.
    El valor representa un porcentaje sobre la renta total,
    por lo que valores fuera de ese rango indican datos corruptos.
    """
    fuera = renta_municipios[
        (renta_municipios["OBS_VALUE"] < 0) |
        (renta_municipios["OBS_VALUE"] > 100)
    ]
    passed = len(fuera) == 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "valores_fuera_rango": MetadataValue.int(len(fuera)),
            "min_valor": MetadataValue.float(float(renta_municipios["OBS_VALUE"].min())),
            "max_valor": MetadataValue.float(float(renta_municipios["OBS_VALUE"].max())),
            "mensaje": MetadataValue.text(
                "OBS_VALUE representa un porcentaje y debe estar entre 0 y 100."
            ),
        },
    )


# ── CHECK 3: VISUALIZACIÓN (asset) ───────────────────────────────────────────

@asset_check(asset=grafico_renta_municipios, additional_ins={"renta_con_nombres": AssetIn()})
def check_series_ordenadas(grafico_renta_municipios, renta_con_nombres):
    """
    Verifica que los municipios están ordenados ascendentemente por OBS_VALUE.
    Usa renta_con_nombres como dependencia adicional para acceder a los datos
    sin tocar el string del path que devuelve el asset.
    """
    ano = renta_con_nombres["TIME_PERIOD_CODE"].max()
    valores = (
        renta_con_nombres[
            (renta_con_nombres["TIME_PERIOD_CODE"] == ano)
            & (renta_con_nombres["MEDIDAS_CODE"] == "SUELDOS_SALARIOS")
        ]
        .dropna(subset=["NOMBRE"])
        .sort_values("OBS_VALUE")["OBS_VALUE"]
        .tolist()
    )
    passed = valores == sorted(valores)

    return AssetCheckResult(
        passed=passed,
        metadata={
            "ordenado": MetadataValue.text("Sí" if passed else "No"),
            "mensaje": MetadataValue.text(
                "Las barras deben estar ordenadas de menor a mayor "
                "para facilitar la comparación entre municipios."
            ),
        },
    )