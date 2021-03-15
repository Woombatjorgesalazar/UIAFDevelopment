
# encoding=UTF-8
#-*- coding: UTF-8 -*-
import os,sys

debub =1

if debub :
    os.environ['SPARK_HOME'] = '/opt/cloudera/parcels/CDH/lib/spark'
    os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
    os.environ['HADOOP_CONF_DIR']='/opt/cloudera/parcels/CDH/lib/spark/conf/yarn-conf:/etc/hive/conf'
    sys.path.append('/opt/cloudera/parcels/CDH/lib/spark/python')
    sys.path.append('/opt/cloudera/parcels/CDH/lib/spark/python/lib/py4j-0.10.7-src.zip')
import common as cm

sql_update_query = """update  public.tparametro set valor=533 where idproceso = 18 and parametro = 'MAX_NUMERO_ENTREGA' """

cm.set_query_bd(sql_update_query,"postgresql-gestioncargues")

import psycopg2


"""connection = psycopg2.connect(user="postgres",
                                      password="postgres",
                                      host="pitagoras.uiaf.gov.co",
                                      port="5432",
                                      database="gestioncargues")
"""
connection = psycopg2.connect(user=cm.get_BDitem("postgresql-gestioncargues", "username"),
                              password=cm.get_BDitem("postgresql-gestioncargues", "password"),
                              host=cm.get_BDitem("postgresql-gestioncargues", "host"),
                              port=cm.get_BDitem("postgresql-gestioncargues", "port"),
                              database=cm.get_BDitem("postgresql-gestioncargues", "Database"))

cursor = connection.cursor()

sql_update_query = """update  public.tparametro set valor=533 where idproceso = 18 and parametro = 'MAX_NUMERO_ENTREGA' """
#cursor.execute(sql_update_query, (533, 18))
cursor.execute(sql_update_query)
connection.commit()
count = cursor.rowcount
print(count, "Record Updated successfully ")