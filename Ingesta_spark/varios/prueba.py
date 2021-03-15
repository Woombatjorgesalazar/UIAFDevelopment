
# encoding=UTF-8
#-*- coding: UTF-8 -*-
from pyspark import SparkContext, SparkConf,HiveContext
import common as cm
import os
import findspark
import configparser


findspark.add_packages('org.postgresql:postgresql:42.2.12') #'mysql:mysql-connector-java:8.0.11')
phatjar=os.path.dirname(os.path.abspath(__file__) )+"/driver/postgresql-42.2.12.jar"
sconf = SparkConf() \
    .setAppName("ETL Cargue DWH_HDFS PRUEBA") \
    .set("spark.jars",phatjar)

sC = SparkContext()
Hc = HiveContext(sC)
sql="(select * from get_proceso('IMPO'))as impo"
#c#cm.kerberos_init("eamadom", "/home/eamadom/eamadom3.keytab")
driver="postgresql"
db_properties = {}
config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__))+ '/CargueDWH.ini')
db_prop = config[driver]

db_properties['user'] = db_prop['username']
db_properties['password'] = db_prop['password']
#db_properties['url'] = db_prop['url']
db_properties['driver'] = db_prop['driver']


df = Hc.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://172.41.10.15:5432/gestioncargues") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", sql) \
    .option("user", "dbauiaf") \
    .option("password", "UIAFdes*") \
    .load()

df.show()

url="jdbc:postgresql://172.41.10.15:5432/gestioncargues"
db_properties={ "driver": "org.postgresql.Driver", 'user': 'dbauiaf','url':""  , "password": "UIAFdes*"}
db_properties={}

db_url = "jdbc:postgresql://172.41.10.15:5432/gestioncargues"
db_properties['user']="dbauiaf"
db_properties['password']="UIAFdes*"
db_properties['url']= ""
db_properties['driver']= "org.postgresql.Driver"


df = Hc.read.jdbc(url=db_url,table=sql,properties=db_properties)

df.show()


tt= cm.get_Query_Postgresql(Hc,sql)

tt.show()


