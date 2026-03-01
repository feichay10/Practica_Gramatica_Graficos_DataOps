import pandas as pd
from dagster import asset, Definitions
from plotnine import ggplot, aes, geom_line, labs, theme_minimal, geom_point

# 1. Asset de Carga de Datos con Renombrado
@asset
def raw_renta_data():
    df = pd.read_csv("data/distribucion-renta-canarias.csv")
    
    # Renombramos las columnas para eliminar los '#' y facilitar el manejo
    df = df.rename(columns={
        'TERRITORIO#es': 'TERRITORIO',
        'TIME_PERIOD#es': 'TIME_PERIOD',
        'MEDIDAS#es': 'MEDIDAS',
        'OBS_VALUE': 'OBS_VALUE'
    })
    
    # Limpieza básica
    return df.dropna(subset=['OBS_VALUE'])

# 2. Asset de Procesamiento
@asset
def renta_filtrada(raw_renta_data):
    df = raw_renta_data
    # Ahora usamos el nombre 'TERRITORIO' ya limpio
    return df[df['TERRITORIO'] == 'Canarias']

# 3. Asset de Visualización (Gramática de Gráficos)
@asset
def grafico_renta(renta_filtrada):
    # Aseguramos que TIME_PERIOD sea numérico para que la línea se dibuje correctamente
    renta_filtrada['TIME_PERIOD'] = pd.to_numeric(renta_filtrada['TIME_PERIOD'])
    
    plot = (
        ggplot(renta_filtrada, aes(x='TIME_PERIOD', y='OBS_VALUE', color='MEDIDAS'))
        + geom_line(size=1.2)
        + geom_point() # Añadimos puntos para ver mejor los años
        + labs(
            title="Evolución de la Distribución de Renta en Canarias",
            subtitle="Fuente: ISTAC",
            x="Año",
            y="Valor",
            color="Concepto"
        )
        + theme_minimal()
    )
    
    # Guardar el gráfico
    plot.save("renta_canarias_plot.png", width=10, height=6, dpi=150)
    return "renta_canarias_plot.png"

# Definición de la instancia de Dagster
defs = Definitions(assets=[raw_renta_data, renta_filtrada, grafico_renta])