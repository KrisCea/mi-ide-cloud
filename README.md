# mi-ide-cloud

Este proyecto procesa datasets reales de TITANIC, Libreria y Clima aplicando transformaciones específicas.

## Fuentes de Datos
- **TITANIC**: Dataset CSV local con información de pasajeros del Titanic.
- **Libreria**: Datos de libros obtenidos desde la API de Open Library (sujeto: fiction).
- **Clima**: Datos de clima en tiempo real obtenidos desde la API de Open-Meteo para Santiago, Chile.

## Transformaciones Aplicadas

1. **TITANIC Survival Summary**: Crea una tabla resumen con el conteo de pasajeros que sobrevivieron o no.

2. **Libreria UniqueKey**: Agrega una columna `UniqueKey` con llave única para cada libro en el dataset Libreria.

3. **Clima Average Temperature**: Calcula el promedio de la temperatura en el dataset Clima y lo almacena como resumen.

4. **TITANIC Filter Age**: Elimina todas las filas de pasajeros menores de 10 años del dataset TITANIC.

## Limpieza y Estandarización
- Eliminación de registros duplicados.
- Eliminación de filas con valores nulos críticos.
- Estandarización de nombres de columnas.
- Conversión de tipos de datos (numéricos, fechas).

## Estructura del Proyecto

- `pipeline.py`: Orquestador principal que carga datos reales, limpia, transforma y guarda resultados.
- `procesamiento/transformacion.py`: Contiene las funciones de transformación.
- `ingestion/`: Módulo para cargar datos desde diferentes fuentes (CSV, APIs).
- `data/processed/`: Carpeta donde se guardan las versiones limpias de los datasets.

## Uso

Ejecutar `python pipeline.py` para procesar los datos y ver el resumen en terminal. Requiere entorno virtual activado con dependencias instaladas.

## Dependencias
- pandas
- requests