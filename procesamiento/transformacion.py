import pandas as pd

def transformacion_1_titanic(almacen_datos):
    """
    Crear una tabla resumen con el conteo de pasajeros que sobrevivieron o no en TITANIC.
    Almacenar resultado como una nueva entrada en almacen_datos.
    """
    titanic = almacen_datos.get('TITANIC', pd.DataFrame())
    if 'Survived' not in titanic.columns:
        resumen = pd.DataFrame(columns=['Survived', 'Count'])
    else:
        resumen = titanic['Survived'].value_counts(dropna=False).reset_index()
        resumen.columns = ['Survived', 'Count']
    almacen_datos['TITANIC_SURVIVAL_SUMMARY'] = resumen

def transformacion_2_libreria(almacen_datos):
    """
    Crear una nueva columna llamada UniqueKey en la entrada Libreria con la llave unica de cada libro.
    Actualizar entrada Libreria con la nueva columna creada.
    """
    libreria = almacen_datos.get('Libreria', pd.DataFrame()).copy()
    if 'Key' in libreria.columns:
        libreria['UniqueKey'] = (
            libreria['Key']
            .astype(str)
            .str.strip()
            .str.replace(r'[^A-Za-z0-9_-]+', '_', regex=True)
            .replace({'': None})
        )
        missing = libreria['UniqueKey'].isna()
        libreria.loc[missing, 'UniqueKey'] = [f"LIB-{i+1:04d}" for i in range(missing.sum())]
    else:
        libreria['UniqueKey'] = [f"LIB-{i+1:04d}" for i in range(len(libreria))]
    almacen_datos['Libreria'] = libreria

def transformacion_3_clima(almacen_datos):
    """
    Crear una tabla resumen con el promedio de la temperatura en Clima.
    Almacenar resultado como una nueva entrada en almacen_datos.
    """
    clima = almacen_datos.get('Clima', pd.DataFrame())
    if 'Temperature' in clima.columns and not clima['Temperature'].dropna().empty:
        promedio_temp = clima['Temperature'].mean()
    else:
        promedio_temp = None
    resumen = pd.DataFrame({'Average_Temperature': [promedio_temp]})
    almacen_datos['CLIMA_AVERAGE_TEMP'] = resumen

def transformacion_4_titanic(almacen_datos):
    """
    Borrar de la entrada TITANIC todas las filas de pasajeros menores de 10 años y actualizar la entrada.
    """
    titanic = almacen_datos.get('TITANIC', pd.DataFrame())
    if 'Age' in titanic.columns:
        titanic_filtrado = titanic[titanic['Age'] >= 10].copy()
    else:
        titanic_filtrado = titanic.copy()
    almacen_datos['TITANIC'] = titanic_filtrado