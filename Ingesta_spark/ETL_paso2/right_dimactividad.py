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
import cruze_right as cr

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
    
    if sigla == 'BDUDA':
        df = cruze_adres(hc, dfcruzar, dicc, actual)


    return df


def cruze_adres(hc, dfcruzar, dicccruzar, actual):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dfcruzar : TYPE: Dataframe de pyspark
        DESCRIPTION: Dataset a cruzar con la tabla principal de hive.
    dicccruzar : TYPE: dict
        DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.
    actual : TYPE: String
        DESCRIPTION: Fecha actual.

    Returns
    -------
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Resultado de dataframe cruzado.

    """
    idsistema = dicccruzar['idsistema']
    idcatalogo = dicccruzar['idcatalogo']
    database = 'dwh_uiaf2'
    namedfprincipal = 'dimactividadeconomicadetallado'
    col_inc = 'idactividadeconomicadetallado'
    adc = ''
    inc = cr.incremental(hc, database, namedfprincipal, col_inc)
    df, dfpt = cr.cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo, adc)
    
    #####################################Procedimiento###################
    colminc = namedfprincipal.replace('dim', '')
   
    
    df = df.withColumn('idactividadeconomica', lit(2).cast(IntegerType())) \
        .withColumn('actividadeconomica', lit('No homologado').cast(StringType())) \
        .withColumn('codigoactividad', lit('Noho').cast(StringType())) \
        .withColumn('idcatalogo', lit(int(idcatalogo)).cast(IntegerType())) \
        .withColumn('idsistema', lit(int(idsistema)).cast(IntegerType())) \
        .withColumn('actividadeconomicasistema', lit(None).cast(StringType())) \
        .withColumn('codigoactividadsistema', lit(None).cast(StringType()))
        
    df = df.withColumn(col_inc, row_number().over(Window.orderBy(monotonically_increasing_id())) + inc)

    return df