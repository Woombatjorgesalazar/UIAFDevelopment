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


def clasificador(hc, dfcruzar, dicc, sigla, actual, actualsin, idcargue):
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
    idcargue : TYPE: integer
        DESCRIPTION: Idcargue para saber a cual encabezado extraer. Se puede generalizar en el futuro.

    Returns
    -------
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Tabla resultado inicial a cargar a dwh_uiaf.

    """
    if sigla == 'TE10':
        df = cruze_efectivo(hc, dfcruzar, dicc, actual, actualsin,  idcargue)
    elif sigla == 'BDUDA':
        df = cruze_adres(hc, dfcruzar, dicc, actual, actualsin, idcargue)


    return df


def numero_registros(hc, encabezado, idcargue):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    encabezado : TYPE: String
        DESCRIPTION: Nombre del encabezado de la tabla de stage_uiaf.
    idcargue : TYPE: integer
        DESCRIPTION: Idcargue para saber a cual encabezado extraer. Se puede generalizar en el futuro.
    Returns
    -------
    num : TYPE: Integer
        DESCRIPTION: Numero con la cantidad de registros de el encabezado.

    """
    sent = "select cantidad_registros from stage_uiaf.{0} \
                 where idcargue = {1}".format(encabezado, idcargue)
    

    df = hc.sql(sent)
    dfp = df.toPandas()
    try:
        num = int(dfp['cantidad_registros'][0])
    except IndexError:
        num = None
    return num

def cruze_efectivo(hc, dfcruzar, dicccruzar, actual, actualsin, idcargue):
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
    actualsin : TYPE: String
        DESCRIPTION: Fecha actual sin guiones.
    idcargue : TYPE: integer
        DESCRIPTION: Idcargue para saber a cual encabezado extraer. Se puede generalizar en el futuro.

    Returns
    -------
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Tabla resultado inicial a cargar a dwh_uiaf.

    """
    idsistema = dicccruzar['idsistema']
    idcatalogo = dicccruzar['idcatalogo']
    database = 'dwh_uiaf2'
    dfdestino = 'dimpersonadetallado'
    namedfprincipal = 'dimpersona'
    col_inc = 'idpersonadetallado'
    adc = ', t2.idfechatransaccion, t2.idfechacargue'
    encabezado = 'te10_encabezado'
    inc = ci.incremental(hc, database, dfdestino, col_inc)


    numreg = numero_registros(hc, encabezado, idcargue)
    

    df, dfpt = ci.cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo, adc)


    df = df.withColumn('idcatalogo', lit(str(idcatalogo)).cast(IntegerType())) \
            .withColumn('idsistema', lit(str(idsistema)).cast(IntegerType())) \
            .withColumn('idtipoidentificacionsistema', col('idtipoidentificacion').cast(StringType())) \
            .withColumn('identificacionsistema', col('identificacion').cast(StringType())) \
            .withColumn('nombresrazonsocialsistema', col('nombresrazonsocial').cast(StringType())) \
            .withColumn('fechanacimientocreacionsistema', lit(0).cast(TimestampType())) \
            .withColumn('tiposgssssistema', lit(0).cast(StringType())) \
            .withColumn('iddanevive', lit(0).cast(StringType())) \
            .withColumn('danevivesistema', lit(0).cast(StringType())) \
            .withColumn('danenacimientosistema', lit(0).cast(StringType())) \
            .withColumn('identregasistema', lit(0).cast(IntegerType())) \
            .withColumn('numeroregsistema', lit(numreg).cast(IntegerType())) \
            .withColumn('estado', lit(0).cast(IntegerType()))
            
    df = df.drop('iddaneresidencia')

    df = df.withColumn('idpersonadetallado', row_number().over(Window.orderBy(monotonically_increasing_id()))+inc)

    #df.show()
    #df.printSchema()
    #exit(0)

    return df

def cruze_adres(hc, dfcruzar, dicc, actual, actualsin, idcargue):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dfcruzar : TYPE: Dataframe de pyspark
        DESCRIPTION: Dataset a cruzar con la tabla principal de hive.
    dicc : TYPE: dict
        DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.
    actual : TYPE: string
        DESCRIPTION: Fecha actual de carga.
    actualsin : TYPE: string
        DESCRIPTION: Fecha actual de carga sin guiones.
    idcargue : TYPE: integer
        DESCRIPTION: Idcargue para saber a cual encabezado extraer. Se puede generalizar en el futuro.


    Returns
    -------
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Resultado de dataframe cruzado.

    """
    idsistema = dicc['idsistema']
    idcatalogo = dicc['idcatalogo']
    database = 'dwh_uiaf2'
    dfdestino = 'dimpersonadetallado'
    namedfprincipal = 'dimpersona'
    col_inc = 'idpersonadetallado'
    adc = ', t2.idfechatransaccion, t2.iddanevive'
    ##############Incremental##############
    inc = ci.incremental(hc, database, dfdestino, col_inc)
    #################Cruze####################
    df, dfpt = ci.cruze_en_detalle(hc, dfcruzar, dicc, database, namedfprincipal, idsistema, idcatalogo, adc)
    
    df = df.withColumn('idpersonadetallado', row_number().over(Window.orderBy(monotonically_increasing_id()))+inc)
    
    df = df.withColumn('idtipoidentificaciondetallado', col('idtipoidentificacion').cast(StringType())) \
        .withColumn('identificacionsistema', col('identificacion').cast(StringType())) \
        .withColumn('nombrerazonsocialsistema', col('nombresrazonsocial').cast(StringType())) \
        .withColumn('fechanacimientocreacionsistema', col('fechanacimientocreacion').cast(TimestampType())) \
        .withColumn('tiposgssssistema', lit(None).cast(StringType())) \
        .withColumn('iddanenacimiento', lit(None).cast(StringType())) \
        .withColumn('danevivesistema', col('iddanevive').cast(StringType())) \
        .withColumn('danenacimientosistema', lit(None).cast(StringType())) \
        .withColumn('idsistema', lit(int(idsistema)).cast(IntegerType())) \
        .withColumn('idcatalogo', lit(int(idcatalogo)).cast(IntegerType())) \
        .withColumn('idfechacargue', lit(int(actualsin.replace('-',''))).cast(IntegerType())) \
        .withColumn('identregadetallado', lit(1).cast(IntegerType())) \
        .withColumn('numeroregsistema', lit(1).cast(IntegerType())) \
        .withColumn('estado', lit(1).cast(IntegerType()))
    
    df = df.drop('iddaneresidencia')
    df = df.withColumn('fechanacimientocreacion', col('fechanacimientocreacion').cast(TimestampType()))
    #df.show()
    #df.printSchema()
    
    #df.write.format("Hive").saveAsTable("%s.%s" % (database, dfdestino), mode="append")
    return df


