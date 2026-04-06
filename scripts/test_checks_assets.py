import os
import pandas as pd
from dagster import asset_check, AssetCheckResult, AssetIn, MetadataValue

from scripts.ampliacion import (
    raw_codislas,
    renta_municipios,
    grafico_renta_municipios,
)

from scripts.test_prompt import (
    codigo_generado_ia,
    visualizacion_png,
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


# ══════════════════════════════════════════════════════════════════════════════
# NUEVOS CHECKS – Pipeline de generación automática con IA (Práctica 4)
# ══════════════════════════════════════════════════════════════════════════════


# ── CHECK 4: SINTAXIS PYTHON VÁLIDA (generación IA) ──────────────────────────

@asset_check(asset=codigo_generado_ia)
def check_codigo_sintaxis(codigo_generado_ia):
    """
    Verifica que el string devuelto por el LLM es Python sintácticamente
    correcto usando compile(). Si el LLM incluyó Markdown residual,
    comentarios mal formados o sintaxis inventada, este check lo detecta
    antes de que exec() lance un error no controlado.
    """
    try:
        compile(codigo_generado_ia, "<string>", "exec")
        passed = True
        msg = "El código compila correctamente."
    except SyntaxError as e:
        passed = False
        msg = f"SyntaxError en línea {e.lineno}: {e.msg}"

    return AssetCheckResult(
        passed=passed,
        metadata={
            "resultado": MetadataValue.text(msg),
            "longitud_codigo": MetadataValue.int(len(codigo_generado_ia)),
            "mensaje": MetadataValue.text(
                "Si falla, el LLM devolvió texto que no es Python válido "
                "(Markdown, explicaciones en lenguaje natural, etc.)."
            ),
        },
    )


# ── CHECK 5: PRESENCIA DE generar_plot ────────────────────────────────────────

@asset_check(asset=codigo_generado_ia)
def check_funcion_generar_plot(codigo_generado_ia):
    """
    Verifica que el código generado contiene la definición de la función
    generar_plot(df) y un return. El asset visualizacion_png recupera la
    función del diccionario de exec() con la clave 'generar_plot'; si la IA
    le dio otro nombre, el pipeline fallaría con KeyError.
    """
    tiene_def    = "def generar_plot" in codigo_generado_ia
    tiene_return = "return" in codigo_generado_ia
    passed = tiene_def and tiene_return

    return AssetCheckResult(
        passed=passed,
        metadata={
            "tiene_def_generar_plot": MetadataValue.text("Sí" if tiene_def else "No"),
            "tiene_return": MetadataValue.text("Sí" if tiene_return else "No"),
            "mensaje": MetadataValue.text(
                "El template exige que la IA defina generar_plot(df) con un return. "
                "Si falta alguno, el gráfico no se generará."
            ),
        },
    )


# ── CHECK 6: FICHERO PNG GENERADO ────────────────────────────────────────────

@asset_check(asset=visualizacion_png)
def check_fichero_png_existe(visualizacion_png):
    """
    Verifica que el fichero PNG generado existe en disco y tiene un tamaño
    mayor a 0 bytes. Un fichero vacío o inexistente indica que plotnine
    falló silenciosamente al guardar, comprometiendo el deploy a GitHub Pages.
    """
    ruta   = visualizacion_png          # el asset devuelve la ruta del fichero
    existe = os.path.isfile(ruta)
    tamano = os.path.getsize(ruta) if existe else 0
    passed = existe and tamano > 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "ruta": MetadataValue.text(ruta),
            "existe": MetadataValue.text("Sí" if existe else "No"),
            "tamano_bytes": MetadataValue.int(tamano),
            "mensaje": MetadataValue.text(
                "El fichero PNG debe existir y tener contenido "
                "para que el deploy a GitHub Pages sea correcto."
            ),
        },
    )