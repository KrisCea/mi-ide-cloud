# mi-ide-cloud

Este proyecto procesa datasets de TITANIC, Libreria y Clima aplicando transformaciones específicas.

## Transformaciones Aplicadas

1. **TITANIC Survival Summary**: Crea una tabla resumen con el conteo de pasajeros que sobrevivieron o no.

2. **Libreria UniqueKey**: Agrega una columna `UniqueKey` con llave única para cada libro en el dataset Libreria.

3. **Clima Average Temperature**: Calcula el promedio de la temperatura en el dataset Clima y lo almacena como resumen.

4. **TITANIC Filter Age**: Elimina todas las filas de pasajeros menores de 10 años del dataset TITANIC.

## Estructura del Proyecto

- `pipeline.py`: Orquestador principal que carga datos, limpia, transforma y guarda resultados.
- `procesamiento/transformacion.py`: Contiene las funciones de transformación.
- `data/processed/`: Carpeta donde se guardan las versiones limpias de los datasets.

## Uso

Ejecutar `python pipeline.py` para procesar los datos y ver el resumen en terminal.