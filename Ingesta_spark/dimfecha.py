<<<<<<< HEAD
from pyspark import SparkContext, HiveContext, SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
import common as cm
import utils
from pyspark.sql.functions import *
import time
import pandas as pd
import numpy as np
=======

import os
import sys

debub =1

if debub :
    os.environ['SPARK_HOME'] = '/opt/cloudera/parcels/CDH/lib/spark'
    os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
    os.environ['HADOOP_CONF_DIR']='/opt/cloudera/parcels/CDH/lib/spark/conf/yarn-conf:/etc/hive/conf'
    sys.path.append('/opt/cloudera/parcels/CDH/lib/spark/python')
    sys.path.append('/opt/cloudera/parcels/CDH/lib/spark/python/lib/py4j-0.10.7-src.zip')


from pyspark import SparkContext, HiveContext, SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql.functions import *
import pandas as pd
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589
from clsFestivos import *

sc = SparkContext()
hc = HiveContext(sc)



def fechas_pandas():
    dicc_dias = {'Monday':'Lunes','Tuesday':'Martes','Wednesday':'Miercoles','Thursday':'Jueves',
     'Friday':'Viernes','Saturday':'Sabado','Sunday':'Domingo'}
    
    dicc_mes = {'01':'Enero','02':'Febrero','03':'Marzo','04':'Abril','05':'Mayo','06':'Junio',
                '07':'Julio','08':'Agosto','09':'Septiembre','10':'Octubre','11':'Noviembre',
                '12':'Diciembre'}
    
    s = pd.date_range(start='1940-01-01', end='2040-01-01')
    df = pd.DataFrame(s, columns=['fecha'])
    
    #df['idfecha'] = df.index + 1
    df['idfecha'] = str
    df['diames'] = int
    df['nombredia'] = str
    df['mesanno'] = str
    df['nombremes'] = str
    df['festivo'] = int
    
    
    for index,i in enumerate(df['fecha']):
        diames = str(i)[str(i).rfind('-')+1:str(i).rfind(' ')]
        df['diames'][index] = str(diames)
        nombredia = df['fecha'][index].strftime('%A')
        nombredia = dicc_dias[nombredia]
        df['nombredia'][index] = nombredia
        mesanno = df['fecha'][index].strftime('%m')
        df['mesanno'][index] = mesanno
        nombremes = dicc_mes[mesanno]
        df['nombremes'][index] = nombremes
        ano = str(i)[:str(i).find('-')]
        festivo = Festivos(int(ano)).esFestivo(int(mesanno),int(diames))
        df['festivo'][index] = int(festivo)
        fecha = str(i)[:str(i).find(' ')]
        df['fecha'][index] = fecha
        df['idfecha'][index] = fecha.replace('-','')
    
    #df.head(100)
    return df
    
df = fechas_pandas()
df['fecha'] = df['fecha'].astype(str)

sdf = hc.createDataFrame(df)


sdf = sdf.withColumn('idfecha',col('idfecha').cast(IntegerType())) \
    .withColumn('diames',col('diames').cast(IntegerType())) \
    .withColumn('festivo',col('festivo').cast(IntegerType()))
    
sdf.show()
sdf.printSchema()
database = 'dwh_uiaf'
dfdestino = 'dimfecha'

<<<<<<< HEAD
#sdf.write.format("Hive").saveAsTable("%s.%s" % (database, dfdestino), mode="append")
=======
sdf.write.format("Hive").saveAsTable("%s.%s" % (database, dfdestino), mode="append")
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589
