from pyspark import SparkContext, HiveContext, SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
import common as cm
import utils
from pyspark.sql.functions import *
import time
import tareas_2 as t2
import tareas_1 as t1
import preprocesamiento as pre

actual = time.strftime("20%y-%m-%d %H:%M:%S")
actualsin = time.strftime("20%y-%m-%d")
actualsin
#######################Contextos de spark y hive################
sc = SparkContext()
hc = HiveContext(sc)

"""
##########################Procesamiento tipoidentificacion####################
dicc = {'idsistema':'3',
 'idcatalogo':'7',
 'campos': ['tipoidentificacion'],
 'prepo': ['segnomafi']
 }

dfcruzar = pre.preproc_tipo_identificacion(hc, dicc)
#t2.cruze_tipo_identificacion_sistema(hc, dfcruzar, dicc)
t1.cruze_tipo_identificacion_sistema(hc, dfcruzar, dicc)
"""

"""
#########################Procesamiento dane############################
dicc = {'idsistema':'3',
 'idcatalogo':'7',
 'campos': ['iddane'],
 'prepo':['nivelsisben','coddepafi']
 }

dfcruzar = pre.preproc_dane(hc, dicc)
#t2.cruze_dane_sistema(hc, dfcruzar, dicc)
t1.cruze_dane_sistema(hc, dfcruzar, dicc)
"""

"""
########################Procesamiento paises###########################
dicc = {'idsistema':'3',
 'idcatalogo':'7'
 }

t2.cruze_paises_sistema(hc, dicc, actual)
t1.cruze_paises_sistema(hc,dfcruzar, dicc, actual)
"""

"""
########################Procesamiento moneda###########################
dicc = {'idsistema':'3',
 'idcatalogo':'7'
 }

t2.cruze_moneda_sistema(hc, dicc, actual)
t1.cruze_moneda_sistema(hc,dfcruzar, dicc)
"""

"""
########################Procesamiento actividad economica####################
dicc = {'idsistema':'3',
 'idcatalogo':'7'
 }

t2.cruze_actividad_sistema(hc, dicc, actual)
t1.cruze_actividad_sistema(hc, dfcruzar, dicc)
"""


########################Procesamiento persona################################
"""
Ayuda: campos = Lista de campos para realizar cruze con tabla de hive
prepo = diccionario de campos para realizar el debido preprocesamiento al dataset
"""
"""
dicc = {'idsistema':'3',
 'idcatalogo':'7',
 'campos': ['identificacion','idtipoidentificacion','fechanacimientocreacion'],
 'prepo': {'iddepartamento':'nivelsisben',
           'idmunicipio':'coddepafi',
           'fechanacimientocreacion':'fecnacafi',
           'identificacion':'tipdocafi',
           'tipoidentificacion':'segnomafi'
          }
 }

dfcruzar = pre.preproc_persona(hc, dicc)
dfcruzar.show()
dfcruzar.printSchema()
#t2.cruze_persona_sistema(hc, dfcruzar, dicc, actual, actualsin)
t1.cruze_persona_sistema(hc, dfcruzar, dicc, actualsin)
"""
#t2.incremental(hc, 'dwh_uiaf', 'dimpersonasistema', 'idpersonasistema')




