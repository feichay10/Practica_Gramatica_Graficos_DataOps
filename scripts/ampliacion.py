import pandas as pd
from plotnine import (
    ggplot, aes, geom_col, geom_bar, geom_point, geom_text,
    facet_wrap, coord_flip, scale_fill_brewer, scale_fill_manual,
    scale_color_brewer, labs, theme, theme_minimal, theme_bw,
    element_text, element_blank, position_stack, position_dodge,
)
from dagster import asset, Definitions, MetadataValue

# ASSET 1 – Carga del dataset principal
@asset
def raw_renta() -> pd.DataFrame:
    df = pd.read_csv("data/distribucion-renta-canarias.csv", sep=",")
    df.columns = [c.split("#")[0] for c in df.columns]   # limpia sufijos #es
    return df


# ASSET 2 – Carga de codigos de islas/municipios
@asset
def raw_codislas() -> pd.DataFrame:
    df = pd.read_csv("data/codislas.csv", sep=";", encoding="latin1")
    # Construye el código de 5 dígitos: CPRO(2) + CMUN(3)
    df["TERRITORIO_CODE"] = (
        df["CPRO"].astype(str).str.zfill(2)
        + df["CMUN"].astype(str).str.zfill(3)
    )
    return df[["TERRITORIO_CODE", "NOMBRE", "ISLA"]]


# ASSET 3 – Carga del fichero de nivel de estudios
@asset
def raw_nivelestudios() -> pd.DataFrame:
    df = pd.read_excel("data/nivelestudios.xlsx")
    df["TERRITORIO_CODE"] = df["Municipios de 500 habitantes o más"].str.extract(r"^(\d{5})")
    df["ANO"] = pd.to_datetime(df["Periodo"]).dt.year
    return df


# ASSET 4 – Limpieza y filtrado de rentas a nivel municipio
@asset
def renta_municipios(raw_renta: pd.DataFrame) -> pd.DataFrame:
    """Filtra solo los registros de municipios (código numérico de 5 dígitos)."""
    df = raw_renta[raw_renta["TERRITORIO_CODE"].str.match(r"^\d{5}$", na=False)].copy()
    df["OBS_VALUE"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
    df = df.dropna(subset=["OBS_VALUE"])
    return df


# ─────────────────────────────────────────────────────────────────────────────
# EJERCICIO 6 – Unión con codislas + visualización con nombres de municipio
# ─────────────────────────────────────────────────────────────────────────────

@asset
def renta_con_nombres(
    renta_municipios: pd.DataFrame,
    raw_codislas: pd.DataFrame,
) -> pd.DataFrame:
    df = renta_municipios.merge(raw_codislas, on="TERRITORIO_CODE", how="left")
    return df


@asset
def grafico_renta_municipios(renta_con_nombres: pd.DataFrame):
    # Último año disponible y medida total (o sueldos)
    ultimo_ano = renta_con_nombres["TIME_PERIOD_CODE"].max()
    df = (
        renta_con_nombres[
            (renta_con_nombres["TIME_PERIOD_CODE"] == ultimo_ano)
            & (renta_con_nombres["MEDIDAS_CODE"] == "SUELDOS_SALARIOS")
        ]
        .dropna(subset=["NOMBRE"])
        .sort_values("OBS_VALUE", ascending=True)
    )

    p = (
        ggplot(df, aes(x="reorder(NOMBRE, OBS_VALUE)", y="OBS_VALUE", fill="ISLA"))
        + geom_col()
        + coord_flip()
        + scale_fill_brewer(type="qual", palette="Set2")
        + labs(
            title=f"Sueldos y salarios por municipio – Canarias {ultimo_ano}",
            x="Municipio",
            y="% sobre la renta total",
            fill="Isla",
        )
        + theme_bw()
        + theme(
            figure_size=(12, 14),
            axis_text_y=element_text(size=7),
            plot_title=element_text(size=12, face="bold"),
        )
    )

    output_path = "grafico_renta_municipios.png"
    p.save(output_path, dpi=150)
    return output_path


# ─────────────────────────────────────────────────────────────────────────────
# EJERCICIO 7 – Integración con nivelestudios.xlsx
# ─────────────────────────────────────────────────────────────────────────────

@asset
def renta_estudios(
    renta_con_nombres: pd.DataFrame,
    raw_nivelestudios: pd.DataFrame,
) -> pd.DataFrame:
    # Renta: sueldos y salarios, último año disponible
    ultimo_ano_renta = renta_con_nombres["TIME_PERIOD_CODE"].max()
    renta_pivot = (
        renta_con_nombres[
            (renta_con_nombres["TIME_PERIOD_CODE"] == ultimo_ano_renta)
            & (renta_con_nombres["MEDIDAS_CODE"] == "SUELDOS_SALARIOS")
        ][["TERRITORIO_CODE", "NOMBRE", "ISLA", "OBS_VALUE"]]
        .rename(columns={"OBS_VALUE": "SUELDOS_SALARIOS"})
    )

    # Nivel de estudios: Total sexo, sin filtrar nacionalidad, último año
    estudios = raw_nivelestudios[
        (raw_nivelestudios["Sexo"] == "Total")
        & (raw_nivelestudios["Nivel de estudios en curso"] != "Total")
        & (raw_nivelestudios["Nivel de estudios en curso"] != "Cursa estudios pero no hay información sobre los mismos")
    ].copy()

    ultimo_ano_est = estudios["ANO"].max()
    estudios = estudios[estudios["ANO"] == ultimo_ano_est]

    # Agrupa por municipio + nivel sumando nacionalidades
    estudios_agg = (
        estudios.groupby(["TERRITORIO_CODE", "Nivel de estudios en curso"], as_index=False)["Total"]
        .sum()
    )

    # Calcula porcentaje dentro de cada municipio
    totales = estudios_agg.groupby("TERRITORIO_CODE")["Total"].transform("sum")
    estudios_agg["PCT_NIVEL"] = estudios_agg["Total"] / totales * 100

    # Une con renta
    df = estudios_agg.merge(renta_pivot, on="TERRITORIO_CODE", how="inner")
    return df


@asset
def grafico_nivel_estudios_por_isla(
    renta_estudios: pd.DataFrame,
    raw_codislas: pd.DataFrame,
) -> str:
    # Etiquetas cortas para el nivel de estudios
    nivel_labels = {
        "Educación primaria e inferior": "Primaria o menos",
        "Primera etapa de Educación Secundaria y similar": "ESO",
        "Segunda etapa de educación secundaria, con orientación general": "Bachillerato",
        "Segunda etapa de Educación Secundaria, con orientación profesional (con y sin continuidad en la educación superior); Educación postsecundaria no superior": "FP / Post-sec.",
        "Educación superior": "Superior",
        "No cursa estudios": "No cursa",
    }
    df = renta_estudios.copy()
    df["Nivel"] = df["Nivel de estudios en curso"].map(nivel_labels).fillna(df["Nivel de estudios en curso"])

    orden_nivel = ["Primaria o menos", "ESO", "Bachillerato", "FP / Post-sec.", "Superior", "No cursa"]
    df["Nivel"] = pd.Categorical(df["Nivel"], categories=orden_nivel, ordered=True)

    p = (
        ggplot(df, aes(x="reorder(NOMBRE, SUELDOS_SALARIOS)", y="PCT_NIVEL", fill="Nivel"))
        + geom_col(position="stack")
        + coord_flip()
        + facet_wrap("~ISLA", scales="free_y", ncol=2)
        + scale_fill_brewer(type="div", palette="RdYlGn")
        + labs(
            title="Distribución del nivel de estudios por municipio y isla – Canarias",
            x="Municipio (ordenado por sueldos y salarios ↑)",
            y="% de la población",
            fill="Nivel de estudios",
        )
        + theme_bw()
        + theme(
            figure_size=(16, 18),
            axis_text_y=element_text(size=6.5),
            strip_text=element_text(size=9, face="bold"),
            legend_position="bottom",
            plot_title=element_text(size=11, face="bold"),
        )
    )

    output_path = "grafico_nivel_estudios_islas.png"
    p.save(output_path, dpi=150)
    return output_path


@asset
def grafico_renta_vs_estudios_superiores(renta_estudios: pd.DataFrame) -> str:
    df = renta_estudios[
        renta_estudios["Nivel de estudios en curso"] == "Educación superior"
    ].copy()

    p = (
        ggplot(df, aes(x="PCT_NIVEL", y="SUELDOS_SALARIOS", color="ISLA"))
        + geom_point(size=3, alpha=0.8)
        + geom_text(
            aes(label="NOMBRE"),
            size=6,
            nudge_y=0.4,
        )
        + scale_color_brewer(type="qual", palette="Dark2")
        + labs(
            title="Relación entre educación superior y sueldos por municipio – Canarias",
            x="% de población con Educación Superior",
            y="Sueldos y salarios (% s/ renta total)",
            color="Isla",
        )
        + theme_minimal()
        + theme(
            figure_size=(13, 9),
            plot_title=element_text(size=11, face="bold"),
        )
    )

    output_path = "grafico_renta_vs_estudios.png"
    p.save(output_path, dpi=150)
    return output_path


# ─────────────────────────────────────────────
# DEFINICIÓN DE DAGSTER
# ─────────────────────────────────────────────
# defs = Definitions(
#     assets=[
#         raw_renta,
#         raw_codislas,
#         raw_nivelestudios,
#         renta_municipios,
#         renta_con_nombres,
#         grafico_renta_municipios,       # Ejercicio 6
#         renta_estudios,
#         grafico_nivel_estudios_por_isla,    # Ejercicio 7a
#         grafico_renta_vs_estudios_superiores,  # Ejercicio 7b
#     ],
# )