import re, requests, pandas as pd, subprocess
from pathlib import Path
from dagster import asset, asset_check, Output, AssetCheckResult, MetadataValue
from plotnine import *

_SCRIPT_DIR = Path(__file__).resolve().parent

# ── Lista de las 7 islas principales ──────────────────────────────────────────
ISLAS = [
    "Tenerife", "Gran Canaria", "Lanzarote", "Fuerteventura",
    "La Palma", "La Gomera", "El Hierro",
]


# ══════════════════════════════════════════════════════════════════════════════
# ASSET 1: INGESTA
# ══════════════════════════════════════════════════════════════════════════════

@asset
def islas_raw():
    """
    Carga el CSV de distribución de rentas de Canarias y filtra únicamente
    las 7 islas principales con la medida 'Sueldos y salarios'.
    Renombra las columnas para simplificar el resto del pipeline.
    """
    df = pd.read_csv(_SCRIPT_DIR / "../data/distribucion-renta-canarias.csv")

    # Filtrar solo las islas (no municipios, ni provincias, ni Canarias)
    df_islas = df[
        (df["TERRITORIO#es"].isin(ISLAS))
        & (df["MEDIDAS_CODE"] == "SUELDOS_SALARIOS")
    ].copy()

    # Renombrar columnas para que el prompt y el código generado sean más limpios
    df_islas = df_islas.rename(columns={
        "TERRITORIO#es": "isla",
        "TIME_PERIOD_CODE": "año",
        "OBS_VALUE": "valor",
    })

    # Quedarnos solo con las columnas necesarias
    df_islas = df_islas[["isla", "año", "valor"]].reset_index(drop=True)

    return Output(
        value=df_islas,
        metadata={
            "variables": MetadataValue.json(list(df_islas.columns)),
            "filas": MetadataValue.int(len(df_islas)),
            "islas": MetadataValue.json(df_islas["isla"].unique().tolist()),
            "rango_años": MetadataValue.text(
                f"{df_islas['año'].min()} - {df_islas['año'].max()}"
            ),
            "mensaje": MetadataValue.text("Dataset filtrado por islas y sueldos/salarios"),
        },
    )


# ══════════════════════════════════════════════════════════════════════════════
# ASSET 2: PLANTILLA IA
# ══════════════════════════════════════════════════════════════════════════════

@asset
def template_ia(islas_raw):
    """
    Construye el payload para la petición al LLM.
    Describe el gráfico usando la gramática de gráficos de Wickham
    para que el LLM genere código plotnine idiomático.
    """
    columnas = ", ".join(islas_raw.columns)
    islas = islas_raw["isla"].unique().tolist()

    # Template que la IA DEBE completar
    template_tecnico = """
def generar_plot(df):
    # El código debe seguir esta estructura:
    # plot = (ggplot(df, aes(...)) + geom_...)
    # return plot
"""

    system_content = (
        "Eres un experto en la gramática de gráficos y Plotnine. "
        "Tu tarea es traducir descripciones en lenguaje natural a código ejecutable. "
        f"Usa siempre este template: {template_tecnico}. "
        "Devuelve exclusivamente el código Python, sin explicaciones ni Markdown."
    )

    descripcion_grafico = f"""
    - Dataset con columnas: {columnas}
    - Islas disponibles: {islas}
    - Estéticas:
        * Variable 'año' mapeada al eje X (numérica).
        * Variable 'valor' mapeada al eje Y.
        * Linea independiente para cada 'isla' (color/group).
    - Geometría: geom_line con tamaño de trazo 1.2.
    - Colores: usar scale_color_manual(values={{
        'Tenerife': '#E74C3C',
        'Gran Canaria': '#3498DB',
        'La Palma': '#2ECC71',
        'La Gomera': '#F39C12',
        'El Hierro': '#9B59B6',
        'Lanzarote': '#1ABC9C',
        'Fuerteventura': '#E67E22'
      }}).
      IMPORTANTE: pasar los colores como argumento 'values' con un diccionario, NO como argumento posicional.
    - Etiquetas:
        * Título: 'Distribución de Sueldos y Salarios por Isla'.
        * Eje X: 'Año'.
        * Eje Y: 'Porcentaje sobre renta total'.
    - Tema: theme_minimal().
    - Leyenda: usar theme(legend_position=(0.85, 0.25)). NO usar strings como 'bottom' o 'right'.
    """

    user_content = (
        f"Basándote en esta descripción, completa el template:\n"
        f"{descripcion_grafico}"
    )

    return {
        "model": "ollama/llama3.1:8b",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content},
        ],
        "temperature": 0.1,
        "stream": False,
    }


# ══════════════════════════════════════════════════════════════════════════════
# ASSET 3: GENERACIÓN DE CÓDIGO
# ══════════════════════════════════════════════════════════════════════════════

@asset
def codigo_generado_ia(context, template_ia):
    """
    Envía la petición al LLM, extrae el código Python de la respuesta
    y lo limpia de posibles artefactos Markdown.
    """
    url = "http://gpu1.esit.ull.es:4000/v1/chat/completions"
    headers = {"Authorization": "Bearer sk-1234"}

    try:
        response = requests.post(
            url, json=template_ia, headers=headers, timeout=60
        )
        response.raise_for_status()

        res_json = response.json()
        codigo_raw = res_json["choices"][0]["message"]["content"]

        # Intentar extraer bloque ```python ... ```
        match = re.search(r"```python\s+(.*?)\s+```", codigo_raw, re.DOTALL)

        if match:
            codigo_final = match.group(1)
        else:
            # Limpiar líneas que no sean código (Markdown headers, listas, etc.)
            lineas = codigo_raw.split("\n")
            lineas_validas = []
            for l in lineas:
                if not l.strip().startswith("###") \
                   and not l.strip().startswith("-") \
                   and not l.strip().startswith("```"):
                    lineas_validas.append(l)
            codigo_final = "\n".join(lineas_validas)

        codigo_final = codigo_final.strip()

        return Output(
            value=codigo_final,
            metadata={
                "codigo_completo": MetadataValue.md(
                    f"```python\n{codigo_final}\n```"
                )
            },
        )

    except Exception as e:
        context.log.error(f"Error en la petición: {e}")
        raise e


# ══════════════════════════════════════════════════════════════════════════════
# ASSET 4: VISUALIZACIÓN
# ══════════════════════════════════════════════════════════════════════════════

@asset
def visualizacion_png(context, codigo_generado_ia, islas_raw):
    """
    Ejecuta el código generado por la IA en un entorno controlado,
    genera el gráfico PNG y lo sube a GitHub Pages automáticamente.
    """
    import plotnine

    # Preparar entorno de ejecución con plotnine y pandas disponibles
    entorno_ejecucion = globals().copy()
    entorno_ejecucion["plotnine"] = plotnine
    entorno_ejecucion.update({
        k: v for k, v in plotnine.__dict__.items()
        if not k.startswith("_")
    })
    entorno_ejecucion["pd"] = pd

    try:
        # Ejecutar el código generado por la IA
        exec(codigo_generado_ia, entorno_ejecucion)

        # Invocar la función generar_plot creada por la IA
        grafico = entorno_ejecucion["generar_plot"](islas_raw)

        # Guardar el gráfico
        ruta_archivo = "visualizacion_ia_1.png"
        grafico.save(ruta_archivo, width=10, height=6, dpi=100)

        # Push automático a GitHub Pages
        subprocess.run(["git", "add", ruta_archivo])
        subprocess.run([
            "git", "commit", "-m",
            "Actualización automática del gráfico"
        ])
        subprocess.run(["git", "push"])

        return Output(
            value=ruta_archivo,
            metadata={
                "ruta": MetadataValue.text(ruta_archivo),
                "mensaje": MetadataValue.text(
                    "Gráfico generado y subido a GitHub Pages"
                ),
            },
        )

    except Exception as e:
        context.log.error(f"Error al renderizar el gráfico: {e}")
        raise e