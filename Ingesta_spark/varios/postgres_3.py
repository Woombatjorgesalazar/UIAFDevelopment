from pyspark import SparkContext, SparkConf,HiveContext, SQLContext
from pyspark.sql import DataFrameReader
import configparser
import os

# Instanciar la configuración de Spark
sconf = SparkConf() \
    .setAppName("ETL Cargue DWH_HDFS ") \
    .set("spark.jars", "/opt/cloudera/parcels/CDH/jars/postgresql-42.2.5.jar")
    
#'tmp/landing/driver'
#'/opt/cloudera/parcels/CDH/jars'

# Crear el contexto para la aplicación Spark
sC = SparkContext(conf=sconf)

# Establecer el nivel del log de la aplicación
sC.setLogLevel('ERROR')
# Crear el context para Hive
Hc = SQLContext(sC)




url = 'jdbc:postgresql://172.41.10.15:5432/gestioncargues'
properties = {'user': 'postgres', 'password': 'postgres', 'driver':'org.postgres.Driver'}
df = DataFrameReader(HiveContext()).jdbc(
    url='jdbc:%s' % url, table='tproceso', properties=properties
)
df.show(10)












