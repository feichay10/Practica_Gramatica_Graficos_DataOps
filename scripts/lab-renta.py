import pandas as pd
from plotnine import ggplot, aes, geom_line, labs, theme_minimal, geom_point

# 1. Carga de Datos con Renombrado
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

# 2. Procesamiento
def renta_filtrada(raw_data):
    df = raw_data
    # Ahora usamos el nombre 'TERRITORIO' ya limpio
    return df[df['TERRITORIO'] == 'Canarias']

# 3. Visualización (Gramática de Gráficos)
def grafico_renta(filtered_data):
    # Aseguramos que TIME_PERIOD sea numérico para que la línea se dibuje correctamente
    filtered_data['TIME_PERIOD'] = pd.to_numeric(filtered_data['TIME_PERIOD'])
    
    plot = (
        ggplot(filtered_data, aes(x='TIME_PERIOD', y='OBS_VALUE', color='MEDIDAS'))
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

# Ejecución principal
if __name__ == "__main__":
    data = raw_renta_data()
    filtered = renta_filtrada(data)
    result = grafico_renta(filtered)
    print(f"Gráfico guardado: {result}")