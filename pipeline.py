import pandas as pd
import os
from procesamiento.transformacion import (
    transformacion_1_titanic,
    transformacion_2_libreria,
    transformacion_3_clima,
    transformacion_4_titanic
)

def crear_datos_ejemplo():
    """
    Crear datos de ejemplo para TITANIC, Libreria y Clima.
    En un proyecto real, estos se cargarían desde archivos o APIs.
    """
    # TITANIC: pequeño dataset de ejemplo
    titanic_data = {
        'PassengerId': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'Survived': [0, 1, 1, 1, 0, 0, 1, 0, 1, 0],
        'Pclass': [3, 1, 3, 1, 3, 3, 1, 3, 3, 2],
        'Name': ['Braund', 'Cumings', 'Heikkinen', 'Futrelle', 'Allen', 'Moran', 'McCarthy', 'Palsson', 'Johnson', 'Nasser'],
        'Sex': ['male', 'female', 'female', 'female', 'male', 'male', 'male', 'male', 'female', 'female'],
        'Age': [22, 38, 26, 35, 35, 25, 54, 2, 27, 14],  # Incluye menores de 10
        'SibSp': [1, 1, 0, 1, 0, 0, 0, 3, 0, 1],
        'Parch': [0, 0, 0, 0, 0, 0, 0, 1, 2, 0],
        'Ticket': ['A/5 21171', 'PC 17599', 'STON/O2. 3101282', '113803', '373450', '330877', '17463', '349909', '347742', '237736'],
        'Fare': [7.25, 71.2833, 7.925, 53.1, 8.05, 8.4583, 51.8625, 21.075, 11.1333, 30.0708],
        'Cabin': [None, 'C85', None, 'C123', None, None, 'E46', None, None, None],
        'Embarked': ['S', 'C', 'S', 'S', 'S', 'Q', 'S', 'S', 'S', 'C']
    }
    df_titanic = pd.DataFrame(titanic_data)

    # Libreria: dataset de libros
    libreria_data = {
        'Title': ['Book1', 'Book2', 'Book3', 'Book4'],
        'Author': ['Author1', 'Author2', 'Author3', 'Author4'],
        'Year': [2000, 2005, 2010, 2015],
        'Genre': ['Fiction', 'Non-Fiction', 'Fiction', 'Sci-Fi']
    }
    df_libreria = pd.DataFrame(libreria_data)

    # Clima: dataset de clima
    clima_data = {
        'Date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'],
        'Temperature': [20.5, 22.1, 19.8, 21.3],
        'Humidity': [60, 65, 55, 70]
    }
    df_clima = pd.DataFrame(clima_data)

    return df_titanic, df_libreria, df_clima

def limpiar_y_estandarizar(df, nombre):
    """
    Identificar y eliminar registros incompletos, duplicados o irrelevantes.
    Estandarizar formatos.
    """
    # Eliminar duplicados
    df = df.drop_duplicates()

    # Eliminar filas con valores nulos críticos
    if nombre == 'TITANIC':
        df = df.dropna(subset=['Survived', 'Age'])
    elif nombre == 'Libreria':
        df = df.dropna(subset=['Title'])
    elif nombre == 'Clima':
        df = df.dropna(subset=['Temperature'])

    # Estandarizar formatos
    if 'Age' in df.columns:
        df['Age'] = pd.to_numeric(df['Age'], errors='coerce')
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    if 'Temperature' in df.columns:
        df['Temperature'] = pd.to_numeric(df['Temperature'], errors='coerce')

    return df

def run_pipeline():
    """
    Orquestador principal: carga datos, limpia, transforma y guarda.
    """
    print("Iniciando pipeline de procesamiento de datos...")

    # Crear datos de ejemplo
    print("Creando datos de ejemplo...")
    df_titanic, df_libreria, df_clima = crear_datos_ejemplo()

    # Limpiar y estandarizar
    print("Limpiando y estandarizando datos...")
    df_titanic = limpiar_y_estandarizar(df_titanic, 'TITANIC')
    df_libreria = limpiar_y_estandarizar(df_libreria, 'Libreria')
    df_clima = limpiar_y_estandarizar(df_clima, 'Clima')

    # Crear almacen_datos
    almacen_datos = {
        'TITANIC': df_titanic,
        'Libreria': df_libreria,
        'Clima': df_clima
    }

    # Ejecutar transformaciones
    print("Ejecutando transformaciones...")
    transformacion_1_titanic(almacen_datos)
    transformacion_2_libreria(almacen_datos)
    transformacion_3_clima(almacen_datos)
    transformacion_4_titanic(almacen_datos)

    # Guardar versiones limpias
    print("Guardando versiones limpias en /data/processed/...")
    os.makedirs('/workspaces/mi-ide-cloud/data/processed', exist_ok=True)
    almacen_datos['TITANIC'].to_csv('/workspaces/mi-ide-cloud/data/processed/titanic_clean.csv', index=False)
    almacen_datos['Libreria'].to_csv('/workspaces/mi-ide-cloud/data/processed/libreria_clean.csv', index=False)
    almacen_datos['Clima'].to_csv('/workspaces/mi-ide-cloud/data/processed/clima_clean.csv', index=False)

    # Imprimir resumen
    print("\nResumen de entradas en almacen_datos:")
    for key, value in almacen_datos.items():
        if isinstance(value, pd.DataFrame):
            print(f"- {key}: {value.shape[0]} filas, {value.shape[1]} columnas")
        else:
            print(f"- {key}: {type(value)}")

    print("\nPipeline completado exitosamente.")

if __name__ == "__main__":
    run_pipeline()