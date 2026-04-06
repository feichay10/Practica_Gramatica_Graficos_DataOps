import json
import csv

# 1. Definir los nombres de los archivos
archivo_geojson = '../data/Municipios-2024.json'
archivo_csv = '../data/municipios_indicadores.csv'

try:
    # 2. Abrir y leer el archivo GeoJSON
    with open(archivo_geojson, 'r', encoding='utf-8') as f:
        datos_geojson = json.load(f)
    
    # 3. Extraer solo el bloque de "properties" de cada municipio
    # En un GeoJSON, los registros están dentro de la lista "features"
    lista_propiedades = [feature.get('properties', {}) for feature in datos_geojson.get('features', [])]
    
    if not lista_propiedades:
        print("No se encontraron datos (features) en el archivo GeoJSON.")
    else:
        # 4. Obtener los nombres de las columnas (cabeceras)
        # Usamos las claves del primer municipio como referencia para las columnas
        columnas = list(lista_propiedades[0].keys())
        
        # 5. Crear y escribir el archivo CSV
        with open(archivo_csv, 'w', newline='', encoding='utf-8') as f:
            # Usamos DictWriter para escribir diccionarios directamente como filas
            writer = csv.DictWriter(f, fieldnames=columnas)
            
            # Escribir la primera fila con los nombres de las columnas
            writer.writeheader()
            
            # Escribir todos los datos de los municipios
            writer.writerows(lista_propiedades)
            
        print(f"¡Éxito! Se han extraído {len(lista_propiedades)} municipios y se han guardado en '{archivo_csv}'.")

except FileNotFoundError:
    print(f"Error: No se ha encontrado el archivo {archivo_geojson}. Asegúrate de que está en la misma carpeta que este script.")
except Exception as e:
    print(f"Ha ocurrido un error inesperado: {e}")