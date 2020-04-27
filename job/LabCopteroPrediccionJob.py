import sys

sys.path.append("C:/Users/gonza/Downloads/LABSpark/")
from pyspark.sql import *
from utils.SparkJob import SparkJob
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os
import statsmodels.formula.api as smf
from datetime import timedelta, date



class LabCopteroPrediccionJob(SparkJob):
    def runJob(sparkSession, s3confPath, s3filePath):
        spark = sparkSession
        conf = s3confPath
        # Carga los dataset
        li = []
        for file in os.listdir(s3filePath):
            print('Reading file: ' + os.path.join("s3filePath: ", file))
            df = pd.read_csv(os.path.join(s3filePath, file))
            li.append(df)
        TraficoTotal = pd.concat(li, axis=0, ignore_index=True)
        TraficoTotal['DT_MEASURE_DATETIME'] = pd.to_datetime(TraficoTotal['DT_MEASURE_DATETIME'])
        TraficoTotal.set_index('DT_MEASURE_DATETIME', inplace=True)  # Lo seteo como indice

        Interfaz1 = TraficoTotal.loc[TraficoTotal.DE_INTERFACE == 'GigabitEthernet0/0']
        G00In = Interfaz1.iloc[:, [3]]
        G00Out = Interfaz1.iloc[:, [4]]

        Interfaz2 = TraficoTotal.loc[TraficoTotal.DE_INTERFACE == 'GigabitEthernet0/0.1100']
        G11In = Interfaz2.iloc[:, [3]]
        G11Out = Interfaz2.iloc[:, [4]]

        upgradePrimerIntervalo = cogerFechas('2018-01', '2018-10', G00In)
        upgradeSegundoIntervalo = cogerFechas('2018-10', '2019-11', G00In)
        upgradeTercerIntervalo = cogerFechas('2019-11', '2020-02', G00In)

        #print("\nUpgrade Primer Intervalo")
        #regresionesPercetiles(upgradePrimerIntervalo, [.1, .2, .3, .5, .6, .7, .8, .9, .95, .96, .97, .98, .99], 3, 12.3, 0.9)
        print("\nUpgrade Segundo Intervalo")
        fechaSupera, predicciones = regresionesPercetiles(upgradeSegundoIntervalo[:-2000], [.90, .95, .96, .97, .98, .99], 3, 16, .82)
        print(fechaSupera)
        print(predicciones)
        #print("\nUpgrade Tercer Intervalo")
        #regresionesPercetiles(upgradeTercerIntervalo, [.1, .2, .3, .5, .6, .7, .8, .9, .95, .96, .97, .98, .99], 3, 12.3, 0.9)


def cogerFechas(fechaMin, fechaMax, serie):
    start_date = pd.to_datetime(fechaMin)
    end_date = pd.to_datetime(fechaMax)
    df = serie
    df['DT_MEASURE_DATETIME'] = df.index.values
    mask = (serie['DT_MEASURE_DATETIME'] > start_date) & (serie['DT_MEASURE_DATETIME'] <= end_date)
    df = serie[mask]
    return df.drop(['DT_MEASURE_DATETIME'], axis=1)


def regresionesPercetiles(serie, cuantiles, meses, caudalM, porcentaje, tipoFun='lineal'):
    # Indices y datos a usar en el modelo
    a = serie.shape[0]
    index = np.arange(1, a + 1)
    # Eje x no puede ser timestamp, por lo que se genera uno auxiliar
    x = np.reshape(index, (a, 1))
    # El eje y son los valores de la serie con los datos del tráfico
    y = serie.iloc[:, [0]].values[:, 0]
    # Guardamos los nuevos datos para el modelo
    data = {'x': x, 'y': y}
    # Multiplicamos el numero de meses por los puntos en un dia y los dias en un mes
    a = a + meses * 288 * 30
    # Valores para la predicción
    z = np.arange(1, a)
    # Creamos una lista de fechas desde la primera de la serie (entendiendo que es la posición 0 del index) hasta la ultima
    # formada por el numero de valores de la serie mas los puntos a predecir
    date_list = [serie.index[0] + timedelta(minutes=5 * x) for x in range(0, a)]
    fechas = pd.to_datetime(date_list)

    # Definicion del modelo
    if tipoFun == 'log':
        mod = smf.quantreg('y ~ I(np.log(x))', data)
    else:
        mod = smf.quantreg('y ~ x', data)
    # Entrenamos el modelo con cada uno de los cuantiles
    res_all = [mod.fit(q=q) for q in cuantiles]

    # Dataframe con los datos a devolver
    fechaSupera = pd.DataFrame({'cuantiles': [], 'fechas': []})
    valoresq = []
    valoresf = []
    # DataFrame con las fechas cuantil y valores
    predicciones = pd.DataFrame({'fechas': fechas[:-1]})
    for qm, res in zip(cuantiles, res_all):
        # Transformo qm a string para que sea el valor de la columna
        predicciones[str(qm)] = res.predict({'x': z})
        # cogemos las predicciones por cada modelo y predecimos
        index = 0
        # Si los valores de prediccion superan el valor caudalMaximo * porcentaje se guardan
        for value in res.predict({'x': z}):
            if value >= caudalM * porcentaje:
                valoresq.append(qm)
                valoresf.append(fechas[index].date())
                break
            index = index + 1
    fechaSupera['cuantiles'] = valoresq
    fechaSupera['fechas'] = valoresf
    predicciones.set_index('fechas', inplace=True)  # Lo seteo como indice

    # Esta funcion en la version final no estara
    #representacionGrafica(serie, predicciones, caudalM, porcentaje)
    return fechaSupera, predicciones

