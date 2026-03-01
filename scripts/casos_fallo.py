import os
import pandas as pd
from dagster import asset, asset_check, AssetCheckResult, AssetIn, MetadataValue, Definitions


# ══════════════════════════════════════════════════════════════════════════════
# CASO 1 – FALLO en check_estandarizacion_islas
# Asset raw_codislas con nombres de isla en minúscula
# ══════════════════════════════════════════════════════════════════════════════

@asset
def raw_codislas_sucio() -> pd.DataFrame:
    df = pd.read_csv("data/codislas.csv", sep=";", encoding="latin1")
    df["TERRITORIO_CODE"] = (
        df["CPRO"].astype(str).str.zfill(2)
        + df["CMUN"].astype(str).str.zfill(3)
    )
    df = df[["TERRITORIO_CODE", "NOMBRE", "ISLA"]]

    # ── FILA SUCIA INTRODUCIDA ────────────────────────────────────────────────
    fila_sucia = pd.DataFrame({
        "TERRITORIO_CODE": ["99999"],
        "NOMBRE":          ["Municipio Ficticio"],
        "ISLA":            ["tenerife"],   # minúscula → fuerza el fallo
    })
    df = pd.concat([df, fila_sucia], ignore_index=True)
    # ─────────────────────────────────────────────────────────────────────────

    return df


@asset_check(asset=raw_codislas_sucio)
def check_estandarizacion_islas_fallo(raw_codislas_sucio):
    originales   = raw_codislas_sucio["ISLA"].nunique()
    normalizadas = raw_codislas_sucio["ISLA"].str.lower().str.strip().nunique()
    passed = originales == normalizadas

    return AssetCheckResult(
        passed=passed,
        metadata={
            "categorias_detectadas": MetadataValue.int(originales),
            "categorias_esperadas":  MetadataValue.int(normalizadas),
            "islas_unicas": MetadataValue.text(
                str(sorted(raw_codislas_sucio["ISLA"].unique()))
            ),
            "principio_gestalt": MetadataValue.text("Similitud – Evitar fragmentación visual"),
            "mensaje": MetadataValue.text(
                "Si hay nombres inconsistentes, plotnine creará leyendas duplicadas."
            ),
        },
    )


# ══════════════════════════════════════════════════════════════════════════════
# CASO 2 – FALLO en check_obs_value_rango
# Asset renta_municipios con valores fuera del rango 0-100
# ══════════════════════════════════════════════════════════════════════════════

@asset
def renta_municipios_sucia() -> pd.DataFrame:
    df = pd.read_csv("data/distribucion-renta-canarias.csv", sep=",")
    df.columns = [c.split("#")[0] for c in df.columns]
    df = df[df["TERRITORIO_CODE"].str.match(r"^\d{5}$", na=False)].copy()
    df["OBS_VALUE"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
    df = df.dropna(subset=["OBS_VALUE"])

    # ── FILA SUCIA INTRODUCIDA ────────────────────────────────────────────────
    fila_sucia = pd.DataFrame({
        "TERRITORIO_CODE": ["99999"],
        "TIME_PERIOD_CODE": [2023],
        "MEDIDAS_CODE":     ["SUELDOS_SALARIOS"],
        "OBS_VALUE":        [-5.0],   # valor negativo → fuerza el fallo
    })
    df = pd.concat([df, fila_sucia], ignore_index=True)
    # ─────────────────────────────────────────────────────────────────────────

    return df


@asset_check(asset=renta_municipios_sucia)
def check_obs_value_rango_fallo(renta_municipios_sucia):
    fuera = renta_municipios_sucia[
        (renta_municipios_sucia["OBS_VALUE"] < 0) |
        (renta_municipios_sucia["OBS_VALUE"] > 100)
    ]
    passed = len(fuera) == 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "valores_fuera_rango": MetadataValue.int(len(fuera)),
            "min_valor": MetadataValue.float(float(renta_municipios_sucia["OBS_VALUE"].min())),
            "max_valor": MetadataValue.float(float(renta_municipios_sucia["OBS_VALUE"].max())),
            "detalle": MetadataValue.md(
                fuera[["TERRITORIO_CODE", "OBS_VALUE"]].head(5).to_markdown(index=False)
            ),
            "mensaje": MetadataValue.text(
                "OBS_VALUE representa un porcentaje y debe estar entre 0 y 100."
            ),
        },
    )


# ══════════════════════════════════════════════════════════════════════════════
# CASO 3 – FALLO en check_series_ordenadas
# Asset grafico con municipios en orden inverso (de mayor a menor)
# ══════════════════════════════════════════════════════════════════════════════

@asset
def renta_con_nombres_desordenada() -> pd.DataFrame:
    df_renta = pd.read_csv("data/distribucion-renta-canarias.csv", sep=",")
    df_renta.columns = [c.split("#")[0] for c in df_renta.columns]
    df_renta = df_renta[df_renta["TERRITORIO_CODE"].str.match(r"^\d{5}$", na=False)].copy()
    df_renta["OBS_VALUE"] = pd.to_numeric(df_renta["OBS_VALUE"], errors="coerce")
    df_renta = df_renta.dropna(subset=["OBS_VALUE"])

    df_codislas = pd.read_csv("data/codislas.csv", sep=";", encoding="latin1")
    df_codislas["TERRITORIO_CODE"] = (
        df_codislas["CPRO"].astype(str).str.zfill(2)
        + df_codislas["CMUN"].astype(str).str.zfill(3)
    )
    df = df_renta.merge(
        df_codislas[["TERRITORIO_CODE", "NOMBRE", "ISLA"]],
        on="TERRITORIO_CODE", how="left"
    )

    # ── FILA SUCIA INTRODUCIDA ────────────────────────────────────────────────
    # Añadimos una fila con OBS_VALUE=999 que rompe el orden ascendente
    # al concatenarse al final del DataFrame
    fila_sucia = pd.DataFrame({
        "TERRITORIO_CODE": ["99999"],
        "TIME_PERIOD_CODE": [df["TIME_PERIOD_CODE"].max()],
        "MEDIDAS_CODE":     ["SUELDOS_SALARIOS"],
        "NOMBRE":           ["Municipio Ficticio"],
        "ISLA":             ["Tenerife"],
        "OBS_VALUE":        [999.0],   # valor enorme al final → rompe el orden
    })
    df = pd.concat([df, fila_sucia], ignore_index=True)
    # ─────────────────────────────────────────────────────────────────────────

    return df


@asset
def grafico_renta_municipios_desordenado(renta_con_nombres_desordenada: pd.DataFrame) -> str:
    """Gráfico con municipios en orden incorrecto (descendente)."""
    from plotnine import (
        ggplot, aes, geom_col, coord_flip,
        scale_fill_brewer, labs, theme_bw, theme, element_text,
    )
    ano = renta_con_nombres_desordenada["TIME_PERIOD_CODE"].max()
    df = (
        renta_con_nombres_desordenada[
            (renta_con_nombres_desordenada["TIME_PERIOD_CODE"] == ano)
            & (renta_con_nombres_desordenada["MEDIDAS_CODE"] == "SUELDOS_SALARIOS")
        ]
        .dropna(subset=["NOMBRE"])
        # ── ERROR: sin reorder, el orden del DataFrame (descendente) se mantiene
    )
    p = (
        ggplot(df, aes(x="NOMBRE", y="OBS_VALUE", fill="ISLA"))
        + geom_col()
        + coord_flip()
        + scale_fill_brewer(type="qual", palette="Set2")
        + labs(title=f"Sueldos por municipio (DESORDENADO) – {ano}",
               x="Municipio", y="% sobre la renta total", fill="Isla")
        + theme_bw()
        + theme(figure_size=(12, 14), axis_text_y=element_text(size=7))
    )
    path = "grafico_renta_municipios_desordenado.png"
    p.save(path, dpi=150)
    return path


@asset_check(asset=grafico_renta_municipios_desordenado, additional_ins={"renta_con_nombres_desordenada": AssetIn()})
def check_series_ordenadas_fallo(grafico_renta_municipios_desordenado, renta_con_nombres_desordenada):
    ano = renta_con_nombres_desordenada["TIME_PERIOD_CODE"].max()
    valores = (
        renta_con_nombres_desordenada[
            (renta_con_nombres_desordenada["TIME_PERIOD_CODE"] == ano)
            & (renta_con_nombres_desordenada["MEDIDAS_CODE"] == "SUELDOS_SALARIOS")
        ]
        .dropna(subset=["NOMBRE"])["OBS_VALUE"]
        .tolist()
    )
    passed = valores == sorted(valores)

    return AssetCheckResult(
        passed=passed,
        metadata={
            "ordenado": MetadataValue.text("Sí" if passed else "No"),
            "primeros_valores": MetadataValue.text(str(valores[:5])),
            "mensaje": MetadataValue.text(
                "Las barras deben estar ordenadas de menor a mayor "
                "para facilitar la comparación entre municipios."
            ),
        },
    )


# ── Definitions para lanzar solo este fichero ─────────────────────────────────
defs = Definitions(
    assets=[
        raw_codislas_sucio,
        renta_municipios_sucia,
        renta_con_nombres_desordenada,
        grafico_renta_municipios_desordenado,
    ],
    asset_checks=[
        check_estandarizacion_islas_fallo,
        check_obs_value_rango_fallo,
        check_series_ordenadas_fallo,
    ],
)