# encoding=UTF-8
#-*- coding: UTF-8 -*-

import sys
import common
import etl
import os

try:
    from pyspark import SparkContext, SparkConf,HiveContext, SQLContext

except ImportError as error:
    print ('Cannot import Pyspark modules')
    raise



if __name__ == '__main__':
    if len(sys.argv) < 2 :
        print("Error en numero de parametros -m sfc.prge -p 2019/04")
        exit (0)
    periodo=0
    print(sys.argv, type(sys.argv),'####################Estos son los argumentos')
    lista = ['/home/woombatcg/Desktop/GitDesarrollo/Ingesta_spark/main.py', '-m', 'IMPO', '-p', '2020/03', '-vvv']
    for i in range(len(lista)):
        if lista[i] == "-m" or  lista[i]=="-modulo":
            sigla = lista[i + 1]
            continue
        if lista[i][0:2] == "-v":
            common.GLOBAL_CONFIG["verbosity"] = len(lista[i])-1
            continue
        if lista[i] == "-p" or  lista[i]=="-periodo":
            periodo = lista[i + 1]
            continue
        

"""
Configuración de la aplicación Spark
"""
# Instanciar la configuración de Spark
sconf = SparkConf() \
    .setAppName("ETL Cargue DWH_HDFS " + sigla) \
    .set("spark.jars", "/opt/cloudera/parcels/CDH/jars/postgresql-42.2.5.jar")
    

#'tmp/landing/driver'
#'/opt/cloudera/parcels/CDH/jars'

# Crear el contexto para la aplicación Spark
sC = SparkContext(conf=sconf)

# Establecer el nivel del log de la aplicación
sC.setLogLevel('ERROR')
# Crear el context para Hive
Hc = HiveContext(sC)
etl.cargar(Hc,sigla,periodo)



