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
    elif sigla == 'BDUDA':
        dfcruzar = preproc_adres(hc, dicc)

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
    idcargue = 157
    sent = """
    select coalesce(t1.tipo_identificacion_titular, t2.tipo_identificacion_realiza) as idtipoidentificacion

    from stage_uiaf.te10_detalles as t1
    full outer join (select tipo_identificacion_realiza
                    from stage_uiaf.te10_detalles) as t2
            on t1.tipo_identificacion_titular = t2.tipo_identificacion_realiza
    where t1.idcargue = {0}
    """.format(str(idcargue))
    dfcruzar = hc.sql(sent)
    
    


    return dfcruzar

def preproc_adres(hc, dicc):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dicc : TYPE: dict
        DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.

    Returns
    -------
    dfcruzar : TYPE: Dataframe de pyspark.
        DESCRIPTION: Dataframe preprocesado manualmente para su posterior cruze.

    """
    ##########################Condicion#############################
    idcargue = utils.last_idcargue(hc, 'ADRES_BDUDA')
    idcargue = 152
    campo = 'segnomafi'
    ####################################Extraer dfcruzar###########################
    sent = "select * from stage_uiaf.adres_bduda where idcargue = {0}".format(str(idcargue))
    
    dfcruzar = hc.sql(sent).groupBy(campo).count()
    ##############################Adaptar cruze con columnas iguales###############################
    dfcruzar = dfcruzar.select(col(campo).alias(dicc['campos'][0]))
    #dfcruzar.registerTempTable("dfcruzar")
    
    return dfcruzar



























