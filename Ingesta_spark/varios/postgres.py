from pyspark import SparkContext, SparkConf,HiveContext, SQLContext
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
Hc = HiveContext(sC)


def get_BDproperties (driver):
    """
    Busca en el archivo .ini la configuracion de la bd segun el driver
    :param driver: driver a usar del .ini
    :return: propiedad de coneccion
    """
    db_properties = {}
    config = configparser.ConfigParser()
    config.read(os.path.dirname(os.path.abspath(__file__))+ '/CargueDWH.ini')
    db_prop = config[driver]
    #db_url = db_prop['url']
    db_properties['user'] = db_prop['username']
    db_properties['password'] = db_prop['password']
    #db_properties['url'] = db_prop['url']
    db_properties['driver'] = db_prop['driver']
    return db_properties

def get_BDurl (driver):
    """
    busca en el archivo .ini la url del servidor que vamos a consutar
    :param driver: driver del servidor que vamos a usar
    :return: url dleservidor
    """
    config = configparser.ConfigParser()
    config.read(os.path.dirname(os.path.abspath(__file__))+ '/CargueDWH.ini')
    db_prop = config[driver]
    db_url = db_prop['url']
    return db_url

jdbcUrl = get_BDurl ('postgresql')

#print('*****', jdbcUrl)
connectionProperties=get_BDproperties ('postgresql')
#prin('++++++', connectionProperties)
pushdown_query = "(select * from tproceso) as proceso"

print('XXXXXXXXXXXXXXXXXXXXXXXXXXX',jdbcUrl, connectionProperties, type(jdbcUrl), type(connectionProperties))
Hc.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties).show(10)
























