import geopandas as gpd
from plotnine import *

gdf = gpd.read_file("./data/Municipios-2024.json")

mapa = (
    ggplot(gdf)
    + geom_map(aes(fill='pact_t'))
    + scale_fill_gradient(low="#f7fbff", high="#08306b",
        name="Tasa de actividad")
    + labs(title="Indicadores laborales por municipio")
    + theme_void()
    + theme(figure_size=(12, 8))
)
mapa.save("mapa_municipios.png", dpi=150)