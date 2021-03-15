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

def clasificador(hc, dfcruzar, dicc, actualsin, sigla):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dfcruzar : TYPE: Dataframe de pyspark.
        DESCRIPTION: Dataframe a cruzar con tabla de dwh_uiaf.
    dicc : TYPE: dict
        DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.
    actualsin : TYPE: String
        DESCRIPTION: Fecha actual sin guiones.
    sigla : TYPE: String
        DESCRIPTION: Palabra clave acerca de el origen de las tablas a cargar.

    Returns
    -------
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Tabla resultado inicial a cargar a dwh_uiaf.

    """
    if sigla == 'TE10':
        df = cruze_efectivo(hc, dfcruzar, dicc, actualsin)


    return df


def cruze_efectivo(hc, dfcruzar, dicccruzar, actualsin):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dfcruzar : TYPE: Dataframe de pyspark.
        DESCRIPTION: Dataframe a cruzar con tabla de dwh_uiaf.
    dicccruzar : TYPE: dict
        DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.
    actualsin : TYPE: String
        DESCRIPTION: Fecha actual sin guiones.

    Returns
    -------
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Tabla resultado inicial a cargar a dwh_uiaf.

    """
    idsistema = dicccruzar['idsistema']
    idcatalogo = dicccruzar['idcatalogo']
    database = 'dwh_uiaf2'
    namedfprincipal = 'dimtipoproductodetallado'
    #dfdestino = 'dimmonedasistema'
    col_inc = 'idtipoproductodetallado'
    adc = ''

    inc = cr.incremental(hc, database, namedfprincipal, col_inc)
    df,dfpt = cr.cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo, adc)

    df = df.withColumn('idtipoproducto', lit(2).cast(IntegerType())) \
    		.withColumn('idtipoproductosistema', col('idtipoproducto').cast(IntegerType())) \
            .withColumn('idfechacargue',lit(actualsin.replace('-','')).cast(IntegerType())) \
            .withColumn('identregatransaccional', lit(0).cast(IntegerType())) \
            .withColumn('numregtransaccional', lit(0).cast(IntegerType())) \
            .withColumn('estado', lit(0).cast(IntegerType())) \
            .withColumn('idfechatransaccion', lit(0).cast(IntegerType())) \
            .withColumn('tipoproductosistema', lit(None).cast(StringType())) \
            .withColumn('tipoproducto', lit('Noho').cast(StringType())) \
            .withColumn('idcatalogo', lit(int(dicccruzar['idcatalogo'])).cast(IntegerType())) \
            .withColumn('idsistema',lit(int(dicccruzar['idsistema'])).cast(IntegerType()))

    df = df.withColumn('idtipoproductodetallado', row_number().over(Window.orderBy(monotonically_increasing_id()))+inc)
    df = df.drop('count')

    return df