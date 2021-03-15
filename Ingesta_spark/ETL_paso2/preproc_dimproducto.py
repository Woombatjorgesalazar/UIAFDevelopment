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


def ultimo_id_cargue(hc, sigla):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    sigla : TYPE: String
        DESCRIPTION: Palabra clave acerca de el origen de las tablas a cargar.

    Returns
    -------
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Tabla con el ultimo idcargado.

    """
    
    sql = "(select idcargue from result_cargue('{0}', 'tabla')) as idcargue".format(sigla)
    df = cm.get_Query_Postgresql(hc, sql)
    return df

def preproc_producto_enc(hc, dicc, sigla):
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
    codent : TYPE: String
        DESCRIPTION: Codigo de la entidad a cargar apartir de un cruce entre stage_uiaf y dimentidad.

    """
    df = ultimo_id_cargue(hc, sigla)
    df = df.toPandas()
    idc = df['idcargue'][0]
    
    sent = """select t2.identidad as identidad
                from stage_uiaf.te10_encabezado as t1
                inner join dwh_uiaf2.dimentidad as t2
                on trim(t1.codigo_entidad) = trim(t2.identificacionentidad)
                where t1.idcargue = {0}""".format(idc)
    dfenc = hc.sql(sent)
    dfenc = dfenc.toPandas()
    try:
        codent = dfenc['identidad'][0]
    except:
        codent = '0'
    return codent


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
        dfcruzar = preproc_efectivo(hc, dicc, sigla)


    return dfcruzar


def preproc_efectivo(hc, dicc, sigla):
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
    idcargue = utils.last_idcargue(hc, 'TE10')
    idcargue = 157
    codent = preproc_producto_enc(hc, dicc, sigla)

    sent = """
                select t1.*, t2.idpersona, {0} as identidad, t3.idtipoproducto
                from stage_uiaf.te10_detalles as t1
                inner join dwh_uiaf2.dimpersona as t2
                on trim(cast(t1.numero_identificacion_titular as string)) = trim(t2.identificacion)
                inner join dwh_uiaf2.dimtipoproducto as t3
                on trim(t1.tipo_producto) = trim(t3.tipoproducto)
                where t1.idcargue = {1}
                """.format(str(codent), str(idcargue))
    dfcruzar = hc.sql(sent)
    
    dfcruzar = dfcruzar.withColumn('numero', col('numero_cuenta').cast(StringType()))
        #.withColumn('identificacionentidad', lit(str(codent)).cast(StringType())) 

    #dfcruzar = dfcruzar.groupBy('numero').count()
    dfcruzar = dfcruzar.select(['numero','fecha_transaccion','idpersona', 'identidad', 'idtipoproducto'])
    
    
    return dfcruzar





