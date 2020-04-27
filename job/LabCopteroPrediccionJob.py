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

        print("\nUpgrade Primer Intervalo")
        regresionesPercetiles(upgradePrimerIntervalo, [.1, .2, .3, .5, .6, .7, .8, .9, .95, .96, .97, .98, .99], 3,
                              12.3, 0.9)
        print("\nUpgrade Segundo Intervalo")
        regresionesPercetiles(upgradeSegundoIntervalo, [.1, .2, .3, .5, .6, .7, .8, .9, .95, .96, .97, .98, .99], 8, 16,
                              0.82)
        print("\nUpgrade Tercer Intervalo")
        regresionesPercetiles(upgradeTercerIntervalo, [.1, .2, .3, .5, .6, .7, .8, .9, .95, .96, .97, .98, .99], 3,
                              12.3, 0.9)


def cogerFechas(fechaMin, fechaMax, serie):
    start_date = pd.to_datetime(fechaMin)
    end_date = pd.to_datetime(fechaMax)
    df = serie
    df['DT_MEASURE_DATETIME'] = df.index.values
    mask = (serie['DT_MEASURE_DATETIME'] > start_date) & (serie['DT_MEASURE_DATETIME'] <= end_date)
    df = serie[mask]
    return df.drop(['DT_MEASURE_DATETIME'], axis=1)


def regresionesPercetiles(serie, quantiles, meses, caudalM, porcentaje):
    # Indices y datos a usar en el modelo
    a = serie.shape[0]
    index = np.arange(1, a + 1)
    # Eje x no puede ser timestamp, por lo que se genera uno auxiliar
    x = np.reshape(index, (a, 1))
    # El eje y son los valores de la serie con los datos del tráfico
    y = serie.iloc[:, [0]].values[:, 0]
    # Guardamos los nuevos datos
    data = {'x': x, 'y': y}
    # Multiplicamos el numero de meses por los puntos en un dia y los dias en un mes
    a = a + meses * 288 * 30
    # Valores para la predicción
    z = np.arange(1, a)
    # Creamos una lista de fechas desde la primera de la serie (entendiendo que es la posición 0 del index) hasta la ultima
    # formada por el numero de valores de la serie mas los puntos a predecir
    date_list = [serie.index[0] + timedelta(minutes=5 * x) for x in range(0, a)]
    fechas = pd.to_datetime(date_list)
    # y = np.reshape(serie['percentil'].values, (a,1))
    # y = serie.iloc[:,[0]].values
    # data = pd.DataFrame({'x': serie.iloc[:,1] , 'y': serie.iloc[:,0]})
    # Definicion del modelo, lineal
    mod = smf.quantreg('y ~ x', data)
    # Entrenamos el modelo con cada uno de los quantiles
    res_all = [mod.fit(q=q) for q in quantiles]
    res_ols = smf.ols('y ~ x', data).fit()
    #plt.figure()
    # Dataframe con los datos a devolver
    calculo = pd.DataFrame({'quantiles': [], 'fecha': []})
    valoresq = []
    valoresf = []
    for qm, res in zip(quantiles, res_all):
        # cogemos las predicciones por cada modelo y predecimos
        #plt.plot(fechas[:-1], res.predict({'x': z}), linestyle='--', lw=1,
        #         color='k', label='q=%.2F' % qm, zorder=2)
        index = 0
        # Si los valores de prediccion superan el valor caudalMaximo * porcentaje se guardan
        for value in res.predict({'x': z}):
            if value >= caudalM * porcentaje:
                valoresq.append(qm)
                valoresf.append(fechas[index].date())
                break
            index = index + 1

    calculo['quantiles'] = valoresq
    calculo['fecha'] = valoresf
    # representacion grafica del valor maximo
    y_max = [[caudalM * porcentaje] * len(fechas[:-1])]
    y_max = np.reshape(y_max, (len(fechas[:-1]), 1))
    #plt.plot(fechas[:-1], y_max, linestyle='--', linewidth=2, color='red')
    # representacion de la serie original
    #plt.plot(serie.index, y, 'o', alpha=.1, zorder=0)
    #plt.show()
    if not calculo.empty:
        print("Resultados para un intervalo de:", meses, "meses.")
        print(calculo)
    else:
        print("No se obtiene ningun cuantil para un intervalo de:", meses, "meses.")
    return calculo

