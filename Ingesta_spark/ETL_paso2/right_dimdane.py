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

def clasificador(hc, dfcruzar, dicc, sigla):
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

    Returns
    -------
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Tabla resultado inicial a cargar a dwh_uiaf.

    """
    if sigla == 'TE10':
        df = cruze_efectivo(hc, dfcruzar, dicc)
    elif sigla == 'BDUDA':
        df = cruze_adres(hc, dfcruzar, dicc)


    return df


def cruze_efectivo(hc, dfcruzar, dicccruzar):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dfcruzar : TYPE: Dataframe de pyspark.
        DESCRIPTION: Dataframe a cruzar con tabla de dwh_uiaf.
    dicccruzar : TYPE: dict
        DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.

    Returns
    -------
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Tabla resultado inicial a cargar a dwh_uiaf.

    """
    
    idsistema = dicccruzar['idsistema']
    idcatalogo = dicccruzar['idcatalogo']
    database = 'dwh_uiaf2'
    namedfprincipal = 'dimdanedetallado'
    #dfdestino = 'dimdanesistema'
    col_inc = 'iddanedetallado'
    adc = ''
    inc = cr.incremental(hc, database, namedfprincipal, col_inc)
    df,dfpt = cr.cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo, adc)
    
    df = df.withColumn('nombremunicipiosistema', lit(None).cast(StringType())) \
        .withColumn('nombredetapartamentosistema', lit(None).cast(StringType())) \
        .withColumn('idsistema', lit(int(idsistema)).cast(IntegerType())) \
        .withColumn('idcatalogo', lit(int(idcatalogo)).cast(IntegerType())) \
        .withColumn('codigodanesistema', col('iddane').cast(StringType())) \
        .withColumn('nombremunicipio', lit('No homologado').cast(StringType())) \
        .withColumn('nombredepartamento', lit('No homologado').cast(StringType())) \
        .withColumn('idpais', lit(10).cast(IntegerType()))
        
    df = df.withColumn(col_inc, row_number().over(Window.orderBy(monotonically_increasing_id()))+inc)
    df = df.drop('count')
    
    return df

def cruze_adres(hc, dfcruzar, dicccruzar):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dfcruzar : TYPE: Dataframe de pyspark
        DESCRIPTION: Dataset a cruzar con la tabla principal de hive.
    dicccruzar : TYPE: dict
        DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.

    Returns
    -------
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Resultado de dataframe cruzado.

    """
    idsistema = dicccruzar['idsistema']
    idcatalogo = dicccruzar['idcatalogo']
    database = 'dwh_uiaf2'
    namedfprincipal = 'dimdanedetallado'
    col_inc = 'iddanedetallado'
    adc = ''
    inc = cr.incremental(hc, database, namedfprincipal, col_inc)
    df, dfpt = cr.cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo, adc)
    
    #####################################Procedimiento###################
    colminc = namedfprincipal.replace('dim', '')
    
    for index,i in enumerate(dfpt['nombre_columna']):
        
        if i.lower() == ('id'+ colminc.replace('sistema', '')):
            df = df.withColumn(i, lit(2).cast(IntegerType()))
    
    df = df.withColumn('nombremunicipio', lit('No homologado').cast(StringType())) \
        .withColumn('nombredepartamento', lit('No homologado').cast(StringType())) \
        .withColumn('idpais', lit(10).cast(IntegerType())) \
        .withColumn('idcatalogo', lit(int(idcatalogo)).cast(IntegerType())) \
        .withColumn('idsistema', lit(int(idsistema)).cast(IntegerType())) \
        .withColumn('nombremunicipiosistema', lit(None).cast(StringType())) \
        .withColumn('nombredetapartamentosistema', lit(None).cast(StringType())) \
        .withColumn('codigodanesistema', col('iddane').cast(StringType()))
    
    df = df.withColumn(col_inc, row_number().over(Window.orderBy(monotonically_increasing_id())) + inc)
    df = df.drop('count')
    
    #df.show()
    #df.printSchema()
    return df
