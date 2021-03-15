from pyspark import SparkContext, HiveContext, SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
from datetime import datetime,date
import common as cm
import utils
from pyspark.sql.functions import *
import time

import gestioncargue





def clasificador(hc, dicc, sigla):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dicc : TYPE: dict
        DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.
    sigla : TYPE: String
        DESCRIPTION: Palabra clave acerca de el origen de las tablas a cargar.

    Returns
    -------
    dfcruzar : TYPE: Dataframe de pyspark
        DESCRIPTION: Tabla a cruzar preprocesada de stage_uiaf.

    """

    if sigla == 'TE10':
        dfcruzar = preproc_efectivo(hc, dicc)


    return dfcruzar


def preproc_efectivo(hc, dicc):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dicc : TYPE: dict
        DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.
    

    Returns
    -------
    dfcruzar : TYPE: Dataframe de pyspark
        DESCRIPTION: Tabla a cruzar preprocesada de stage_uiaf.

    """
    idcargue = utils.last_idcargue(hc, 'TE10')
    #idcargue = 157
    sent = "select * from stage_uiaf.te10_detalles where idcargue = {0}".format(str(idcargue))
    dfcruzar = hc.sql(sent)
    
    dfcruzar = dfcruzar.withColumn('idtipoproducto', col('tipo_producto').cast(StringType()))
    dfcruzar = dfcruzar.groupBy('idtipoproducto').count()
    #dfcruzar.show()
    #dfcruzar.printSchema()
    
    return dfcruzar


































