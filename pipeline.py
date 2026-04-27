import pandas as pd
import os
from procesamiento.transformacion import (
    transformacion_1_titanic,
    transformacion_2_libreria,
    transformacion_3_clima,
    transformacion_4_titanic
)
from ingestion.lectura_csv import leer_datos_csv
from ingestion.leer_batch import leer_datos_batch
from ingestion.fuente_realtime import leer_clima_tiempo_real
import time

def cargar_datos_reales():
    """
    Cargar datos reales usando las funciones del ejemplo del profesor.
    """
    almacen_datos = {}

    print("--- Lectura de csv TITANIC")
    almacen_datos['TITANIC'] = leer_datos_csv()

    print("--- Lectura de titulos libros (Libreria)")
    almacen_datos['Libreria'] = leer_datos_batch('fiction')  # Cambié a 'fiction' para más variedad

    print("--- Lectura del clima en tiempo real (Clima)")
    total_lecturas = []

    # Tomamos 5 instantáneas para simular tiempo real
    for i in range(5):
        print(f"  > instantanea {i+1}...")
        df_snap = leer_clima_tiempo_real()
        if not df_snap.empty:
            total_lecturas.append(df_snap)
        time.sleep(1)  # Short delay

    if total_lecturas:
        almacen_datos['Clima'] = pd.concat(total_lecturas, ignore_index=True)
    else:
        almacen_datos['Clima'] = pd.DataFrame()

    return almacen_datos

def limpiar_y_estandarizar(df, nombre):
    """
    Identificar y eliminar registros incompletos, duplicados o irrelevantes.
    Estandarizar formatos y nombres de columnas.
    """
    df = df.copy()

    # Estandarizar nombres de columnas según el dataset
    if nombre == 'TITANIC':
        rename_dict = {
            '2urvived': 'Survived',
            'Passengerid': 'PassengerId',
            'sibsp': 'SibSp',
            'sex': 'Sex',
            'pclass': 'Pclass',
            'name': 'Name',
            'age': 'Age',
            'fare': 'Fare',
            'embarked': 'Embarked'
        }
        df = df.rename(columns=rename_dict)
        # Eliminar columnas irrelevantes
        skip_cols = [col for col in df.columns if col.lower().startswith('zero') or col.lower().startswith('unnamed')]
        df = df.drop(columns=skip_cols, errors='ignore')
        subset_cols = [col for col in ['Survived', 'Age'] if col in df.columns]
    elif nombre == 'Libreria':
        rename_dict = {
            'title': 'Title',
            'key': 'Key',
            'first_publish_year': 'Year'
        }
        df = df.rename(columns=rename_dict)
        subset_cols = [col for col in ['Title', 'Key'] if col in df.columns]
    elif nombre == 'Clima':
        rename_dict = {
            'temperature': 'Temperature',
            'time': 'time',
            'windspeed': 'windspeed',
            'winddirection': 'winddirection'
        }
        df = df.rename(columns=rename_dict)
        subset_cols = ['Temperature'] if 'Temperature' in df.columns else []
    else:
        subset_cols = []

    # Eliminar duplicados e incompletos
    df = df.drop_duplicates(ignore_index=True)
    if subset_cols:
        df = df.dropna(subset=subset_cols)

    # Tipos de datos y estandarización
    if 'Age' in df.columns:
        df['Age'] = pd.to_numeric(df['Age'], errors='coerce')
        df = df[df['Age'] >= 0]
    if 'Survived' in df.columns:
        df['Survived'] = pd.to_numeric(df['Survived'], errors='coerce').astype('Int64')
    if 'Year' in df.columns:
        df['Year'] = pd.to_numeric(df['Year'], errors='coerce').astype('Int64')
    if 'Temperature' in df.columns:
        df['Temperature'] = pd.to_numeric(df['Temperature'], errors='coerce')
    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'], errors='coerce')
    for text_col in df.select_dtypes(include=['object', 'string']).columns:
        df[text_col] = df[text_col].astype(str).str.strip()
        if text_col in ['Name', 'Title', 'Sex', 'Embarked']:
            df[text_col] = df[text_col].str.title()

    return df

def guardar_versiones_limpias(almacen_datos, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    for key, value in almacen_datos.items():
        if isinstance(value, pd.DataFrame) and key in ['TITANIC', 'Libreria', 'Clima']:
            file_path = os.path.join(output_dir, f"{key.lower()}_clean.csv")
            value.to_csv(file_path, index=False)
            print(f"  Guardado: {file_path}")


def run_pipeline():
    """
    Orquestador principal: carga datos reales, limpia, transforma y guarda.
    """
    print("Iniciando pipeline de procesamiento de datos...")

    # Cargar datos reales
    print("Cargando datos reales...")
    almacen_datos = cargar_datos_reales()

    print("--- Resumen de datos sin transformar")
    for elemento, df in almacen_datos.items():
        print(f"\n📍 FUENTE: {elemento}")
        if not df.empty:
            print(f"Rows: {len(df)} | Columns: {list(df.columns)}")
            print(df.head(2))
        else:
            print("Empty Table (Check connection)")

    # Limpiar y estandarizar
    print("\nLimpiando y estandarizando datos...")
    for nombre, df in almacen_datos.items():
        almacen_datos[nombre] = limpiar_y_estandarizar(df, nombre)

    # Ejecutar transformaciones
    print("Ejecutando transformaciones...")
    transformacion_1_titanic(almacen_datos)
    transformacion_2_libreria(almacen_datos)
    transformacion_3_clima(almacen_datos)
    transformacion_4_titanic(almacen_datos)

    # Guardar versiones limpias
    print("Guardando versiones limpias en /data/processed/...")
    guardar_versiones_limpias(almacen_datos, '/workspaces/mi-ide-cloud/data/processed')

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