from pyspark import SparkContext, HiveContext, SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType
from pyspark.sql.functions import desc, row_number, monotonically_increasing_id
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
import common as cm
import utils
from pyspark.sql.functions import *
import time
import cruze_inner as ci





def clasificador(hc, dfcruzar, dicc, sigla, actual):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dfcruzar : TYPE: Dataframe de pyspark.
        DESCRIPTION: Dataframe a cruzar con tabla de dwh_uiaf.
    dicc : TYPE: dict
        DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.
    sigla : TYPE: String
        DESCRIPTION: Palabra clave acerca de el origen de las tablas a cargar.
    actual : TYPE: String
        DESCRIPTION: Fecha actual de carga.

    Returns
    -------
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Tabla resultado inicial a cargar a dwh_uiaf.

    """
    
    if sigla == 'TE10':
        df = cruze_efectivo(hc, dfcruzar, dicc, actual)
    elif sigla == 'BDUDA':
        df = cruze_adres(hc, dfcruzar, dicc, actual)


    return df

def cruze_efectivo(hc, dfcruzar, dicccruzar, actual):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dfcruzar : TYPE: Dataframe de pyspark.
        DESCRIPTION: Dataframe a cruzar con tabla de dwh_uiaf.
    dicccruzar : TYPE: dict
        DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.
    actual : TYPE: String
        DESCRIPTION: Fecha actual de carga.

    Returns
    -------
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Tabla resultado inicial a cargar a dwh_uiaf.

    """
    idsistema = dicccruzar['idsistema']
    idcatalogo = dicccruzar['idcatalogo']
    database = 'dwh_uiaf2'
    namedfprincipal = 'dimmoneda'
    dfdestino = 'dimmonedadetallado'
    col_inc = 'idmonedadetallado'
    adc = ''
    inc = ci.incremental(hc, database, dfdestino, col_inc)
    df,dfpt = ci.cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo, adc)
    
    df = df.withColumn('idcatalogo', lit(int(dicccruzar['idcatalogo'])).cast(IntegerType())) \
        .withColumn('idsistema',lit(int(dicccruzar['idsistema'])).cast(IntegerType())) \
        .withColumn('codigosistema', col('codigo').cast(StringType())) \
        .withColumn('monedasistema', lit(None).cast(StringType()))
        
    df = df.withColumn('idmonedadetallado', row_number().over(Window.orderBy(monotonically_increasing_id()))+inc)
    
    #df.show()
    #df.printSchema()
    return df


def cruze_adres(hc, df, dicc, actual):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dicc : TYPE: dict
        DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.
    actual : TYPE: string
        DESCRIPTION: Fecha actual de carga.

    Returns
    -------
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Resultado de dataframe cruzado.

    """
    database = 'dwh_uiaf2'
    dfdestino = 'dimmonedadetallado'
    
    col_inc = 'idmonedadetallado'
    inc = ci.incremental(hc, database, dfdestino, col_inc)
    
    
    df = df.withColumn('idcatalogo', lit(int(dicc['idcatalogo'])).cast(IntegerType())) \
        .withColumn('idsistema',lit(int(dicc['idsistema'])).cast(IntegerType())) \
        .withColumn('codigosistema',lit(None).cast(StringType())) \
        .withColumn('monedasistema', lit(None).cast(StringType()))
    df = df.withColumn('idmonedadetallado', row_number().over(Window.orderBy(monotonically_increasing_id()))+inc)
    
    #df.show()
    #df.printSchema()
    #df.write.format("Hive").saveAsTable("%s.%s" % (database, dfdestino), mode="append")
    return df