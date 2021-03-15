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
    
    if sigla == 'BDUDA':
        df = cruze_adres(hc, dfcruzar, dicc, actual)


    return df


def cruze_adres(hc, df, dicc, actual):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Dataframe para armar la tabla a dwh_uiaf.
    dicc : TYPE: dict
        DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.
    actual : TYPE: string
        DESCRIPTION: Fecha actual para realizar el cargue.

    Returns
    -------
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Resultado de dataframe cruzado.

    """
    database = 'dwh_uiaf2'
    dfdestino = 'dimpaisesdetallado'
    
    col_inc = 'idpaisesdetallado'
    inc = ci.incremental(hc, database, dfdestino, col_inc)
    
    df = df.withColumn('idcatalogo',lit(int(dicc['idcatalogo'])).cast(IntegerType())) \
        .withColumn('idsistema',lit(int(dicc['idsistema'])).cast(IntegerType())) \
        .withColumn('nombresistema',lit(None).cast(StringType())) \
        .withColumn('moneda',lit('COP').cast(StringType())) \
        .withColumn('fechacarga',lit(actual).cast(StringType())) \
        .withColumn('codigopaissistema',lit(None).cast(StringType())) \
        .withColumn('observacion',lit('Este se esta cargando manual').cast(StringType())) \
        .drop('fechamodificacion')
    df = df.withColumn('idpaisesdetallado', row_number().over(Window.orderBy(monotonically_increasing_id()))+inc)
    
    #df.show()
    #df.printSchema()
    #df.write.format("Hive").saveAsTable("%s.%s" % (database, dfdestino), mode="append")
    return df
