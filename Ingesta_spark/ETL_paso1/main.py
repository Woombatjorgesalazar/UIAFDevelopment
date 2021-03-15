# encoding=UTF-8
#-*- coding: UTF-8 -*-

import sys
import os

debub =0

if debub :
    os.environ['SPARK_HOME'] = '/opt/cloudera/parcels/CDH/lib/spark'
    os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
    os.environ['HADOOP_CONF_DIR']='/opt/cloudera/parcels/CDH/lib/spark/conf/yarn-conf:/etc/hive/conf'
    sys.path.append('/opt/cloudera/parcels/CDH/lib/spark/python')
    sys.path.append('/opt/cloudera/parcels/CDH/lib/spark/python/lib/py4j-0.10.7-src.zip')

import common
import etl
<<<<<<< HEAD:ETL_paso1/main.py
import os
import common as cm
=======
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589:main.py

try:
    from pyspark import SparkContext, SparkConf,HiveContext

except ImportError as error:
    print ('Cannot import Pyspark modules')
    raise



if __name__ == '__main__':
    if len(sys.argv) < 2 :
        print("Error en numero de parametros -m sfc.prge -p 2019/04")
        exit (0)
    periodo=0
    for i in range(len(sys.argv)):
        if sys.argv[i] == "-m" or  sys.argv[i]=="-modulo":
            sigla = sys.argv[i + 1]
            continue
        if sys.argv[i][0:2] == "-v":
            common.GLOBAL_CONFIG["verbosity"] = len(sys.argv[i])-1
            continue
        if sys.argv[i] == "-p" or  sys.argv[i]=="-periodo":
            periodo = sys.argv[i + 1]
            continue

if  cm.GLOBAL_CONFIG.get("local"):
    import findspark
    findspark.add_packages('org.postgresql:postgresql:42.2.12')

"""
Configuración de la aplicación Spark
"""
# Instanciar la configuración de Spark
sconf = SparkConf() \
    .setAppName("ETL Cargue DWH_HDFS " + sigla) \
<<<<<<< HEAD:ETL_paso1/main.py
    .set("spark.jars", os.path.dirname(os.path.abspath(__file__) )+"/driver/postgresql-42.2.12.jar")
=======
    .set("spark.jars", "/home/proyectos/Ingesta_spark/driver/postgresql-42.2.12.jar")
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589:main.py


# Crear el contexto para la aplicación Spark
sC = SparkContext(conf=sconf)

# Establecer el nivel del log de la aplicación
sC.setLogLevel('ERROR')
# Crear el context para Hive
Hc = HiveContext(sC)
print('#############################Inicio##################')
etl.cargar(Hc,sigla,periodo)




