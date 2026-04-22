import pandas as pd

def transformacion_1_titanic(almacen_datos):
    """
    Crear una tabla resumen con el conteo de pasajeros que sobrevivieron o no en TITANIC.
    Almacenar resultado como una nueva entrada en almacen_datos.
    """
    titanic = almacen_datos['TITANIC']
    resumen = titanic['Survived'].value_counts().reset_index()
    resumen.columns = ['Survived', 'Count']
    almacen_datos['TITANIC_SURVIVAL_SUMMARY'] = resumen

def transformacion_2_libreria(almacen_datos):
    """
    Crear una nueva columna llamada UniqueKey en la entrada Libreria con la llave unica de cada libro.
    Actualizar entrada Libreria con la nueva columna creada.
    """
    libreria = almacen_datos['Libreria']
    libreria['UniqueKey'] = range(1, len(libreria) + 1)
    almacen_datos['Libreria'] = libreria

def transformacion_3_clima(almacen_datos):
    """
    Crear una tabla resumen con el promedio de la temperatura en Clima.
    Almacenar resultado como una nueva entrada en almacen_datos.
    """
    clima = almacen_datos['Clima']
    promedio_temp = clima['Temperature'].mean()
    resumen = pd.DataFrame({'Average_Temperature': [promedio_temp]})
    almacen_datos['CLIMA_AVERAGE_TEMP'] = resumen

def transformacion_4_titanic(almacen_datos):
    """
    Borrar de la entrada TITANIC todas las filas de pasajeros menores de 10 años y actualizar la entrada.
    """
    titanic = almacen_datos['TITANIC']
    titanic_filtrado = titanic[titanic['Age'] >= 10]
    almacen_datos['TITANIC'] = titanic_filtrado