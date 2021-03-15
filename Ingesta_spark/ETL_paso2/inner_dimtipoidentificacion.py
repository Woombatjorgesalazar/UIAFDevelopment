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




def clasificador(hc, dfcruzar, dicc, sigla, actual, actualsin):
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
    actualsin : TYPE: String
        DESCRIPTION: Fecha actual sin guiones.

    Returns
    -------
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Tabla resultado inicial a cargar a dwh_uiaf.

    """
    if sigla == 'TE10':
        df = cruze_efectivo(hc, dfcruzar, dicc, actual, actualsin)
    elif sigla == 'BDUDA':
        df= cruze_adres(hc, dfcruzar, dicc, actual, actualsin)


    return df

def cruze_efectivo(hc, dfcruzar, dicccruzar, actual, actualsin):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dfcruzar : TYPE: Dataframe de pyspark.
        DESCRIPTION: Dataframe a cruzar con tabla de dwh_uiaf.
    dicc : TYPE: dict
        DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.
    actual : TYPE: String
        DESCRIPTION: Fecha actual de carga.
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
    dfdestino = 'dimtipoidentificaciondetallado'
    namedfprincipal = 'dimtipoidentificacion'
    col_inc = 'idtipoidentificaciondetallado'
    adc = ''
    inc = ci.incremental(hc, database, dfdestino, col_inc)
    
    df, dfpt = ci.cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo, adc)
    
    df = df.withColumn('tipoidentificacionsistema', lit(None).cast(StringType())) \
            .withColumn('descripcionsistema', lit(None).cast(StringType())) \
            .withColumn('idcatalogo', lit(str(idcatalogo)).cast(IntegerType())) \
            .withColumn('idsistema', lit(str(idsistema)).cast(IntegerType())) \
            .withColumn('observaciones', lit(None).cast(StringType()))
    
    
    df = df.withColumn('idtipoidentificaciondetallado', row_number().over(Window.orderBy(monotonically_increasing_id()))+inc)
    #df.show()
    #df.printSchema()
    return df


def cruze_adres(hc, dfcruzar, dicccruzar, actual, actualsin):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dfcruzar : TYPE: Dataframe de pyspark
        DESCRIPTION:Dataset a cruzar con la tabla principal de hive.
    dicccruzar : TYPE: dict
        DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.
    actual : TYPE: String
        DESCRIPTION: Fecha actual de carga.
    actualsin : TYPE: String
        DESCRIPTION: Fecha actual sin guiones.
    Returns
    -------
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Resultado de dataframe cruzado.

    """
    idsistema = dicccruzar['idsistema']
    idcatalogo = dicccruzar['idcatalogo']
    database = 'dwh_uiaf2'
    namedfprincipal = 'dimtipoidentificacion'
    dfdestino = 'dimtipoidentificaciondetallado'
    col_inc = 'idtipoidentificaciondetallado'
    adc = ''
    inc = ci.incremental(hc, database, dfdestino, col_inc)
    df, dfpt = ci.cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo, adc)
    
    
    
    df = df.withColumn('tipoidentificacionsistema', col('tipoidentificacion')) \
    .withColumn('descripcionsistema', lit(None).cast(StringType())) \
    .withColumn('idcatalogo', lit(int(idcatalogo)).cast(IntegerType())) \
    .withColumn('idsistema', lit(int(idsistema)).cast(IntegerType())) \
    .withColumn('observaciones', lit('Carga semiautomatica').cast(StringType())) \
    .withColumn('idtipoidentificacionsistema', col('idtipoidentificacion').cast(IntegerType()))
    ##########################ID sistema##################################################
    df = df.withColumn('idtipoidentificaciondetallado', row_number().over(Window.orderBy(monotonically_increasing_id())) + inc)
    
    #df.printSchema()
    #df.show()
    #df.write.format("Hive").saveAsTable("%s.%s" % (database, dfdestino), mode="append")
    return df
