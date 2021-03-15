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
        df = cruze_efectivo(hc, dfcruzar, dicc, actual, actualsin, idcargue)
    elif sigla == 'BDUDA':
        df = cruze_adres(hc, dfcruzar, dicc, actual, actualsin, idcargue)


    return df

def cruze_adres(hc, dfcruzar, dicccruzar, actual, actualsin, idcargue):
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
    namedfprincipal = 'dimpersonadetallado'
    col_inc = 'idpersonadetallado'
    adc = ''
    inc = cr.incremental(hc, database, namedfprincipal, col_inc)
    df, dfpt = cr.cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo, adc)
    
    #####################################Procedimiento###################
    colminc = namedfprincipal.replace('dim', '')
    """
    for index,i in enumerate(dfpt['nombre_columna']):
        
        if i.lower() == ('id'+ colminc.replace('sistema', '')):
            df = df.withColumn(i, lit(2).cast(IntegerType()))
    """
    
    
    df = df.withColumn('idpersona', lit(2).cast(IntegerType())) \
        .withColumn('idtipoidentificaciondetallado', col('idtipoidentificacion').cast(StringType())) \
        .withColumn('idtipoidentificacion', lit(2).cast(IntegerType())) \
        .withColumn('identificacionsistema', col('identificacion').cast(StringType())) \
        .withColumn('identificacion', lit('No homologado').cast(StringType())) \
        .withColumn('nombrerazonsocialsistema', col('nombresrazonsocial').cast(StringType())) \
        .withColumn('nombresrazonsocial', lit('No homologado').cast(StringType())) \
        .withColumn('fechanacimientocreacionsistema', col('fechanacimientocreacion').cast(StringType())) \
        .withColumn('fechanacimientocreacion', lit('1900-01-01 00:00:00').cast(StringType())) \
        .withColumn('idactividadeconomica', lit(2).cast(IntegerType())) \
        .withColumn('tiposgssssistema', lit(None).cast(StringType())) \
        .withColumn('idtiposgsss', lit(2).cast(IntegerType())) \
        .withColumn('danevivesistema', col('iddanevive').cast(StringType())) \
        .withColumn('iddanenacimiento', lit(2).cast(IntegerType())) \
        .withColumn('danenacimientosistema', lit(None).cast(StringType())) \
        .withColumn('idcatalogo', lit(int(idcatalogo)).cast(IntegerType())) \
        .withColumn('idsistema', lit(int(idsistema)).cast(IntegerType())) \
        .withColumn('idfechacargue', lit(int(actualsin.replace('-',''))).cast(IntegerType())) \
        .withColumn('identregadetallado', lit(1).cast(IntegerType())) \
        .withColumn('numeroregsistema', lit(1).cast(IntegerType())) \
        .withColumn('estado', lit(1).cast(IntegerType())) \
        .withColumn('idroluiaf', lit(11).cast(IntegerType()))
    
    df = df.withColumn(col_inc, row_number().over(Window.orderBy(monotonically_increasing_id())) + inc)
    
    #df.show()
    #df.printSchema()
    return df


def numero_registros(hc, encabezado, idcargue):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    encabezado : TYPE: String
        DESCRIPTION: Nombre del encabezado de stage_uiaf.
    idcargue : TYPE: integer
        DESCRIPTION: Idcargue para saber a cual encabezado extraer. Se puede generalizar en el futuro.

    Returns
    -------
    num : TYPE: Integer
        DESCRIPTION: Cantidadd de registros de las tablas con encabezado.

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
    #dfdestino = 'dimpersonasistema'
    namedfprincipal = 'dimpersonadetallado'
    col_inc = 'idpersonadetallado'
    adc = ''
    encabezado = 'te10_encabezado'
    inc = cr.incremental(hc, database, namedfprincipal, col_inc)


    numreg = numero_registros(hc, encabezado, idcargue)

    df, dfpt = cr.cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo, adc)

    df = df.withColumn('idpersona', lit(2).cast(IntegerType())) \
        .withColumn('idtipoidentificaciondetallado', col('idtipoidentificacion').cast(IntegerType())) \
        .withColumn('identificacionsistema', col('identificacion').cast(StringType())) \
        .withColumn('nombrerazonsocialsistema', col('nombresrazonsocial').cast(StringType())) \
        .withColumn('fechanacimientocreacion', lit(0).cast(TimestampType())) \
        .withColumn('fechanacimientocreacionsistema', lit(0).cast(TimestampType())) \
        .withColumn('idactividadeconomica', lit(0).cast(IntegerType())) \
        .withColumn('idtiposgsss', lit(0).cast(IntegerType())) \
        .withColumn('tiposgssssistema', lit(0).cast(StringType())) \
        .withColumn('iddanevive', lit(0).cast(StringType())) \
        .withColumn('danevivesistema', lit(0).cast(StringType())) \
        .withColumn('iddanenacimiento', lit(0).cast(IntegerType())) \
        .withColumn('danenacimientosistema', lit(0).cast(StringType())) \
        .withColumn('idcatalogo', lit(str(idcatalogo)).cast(IntegerType())) \
        .withColumn('idsistema', lit(str(idsistema)).cast(IntegerType())) \
        .withColumn('identregasistema', lit(0).cast(IntegerType())) \
        .withColumn('numeroregsistema', lit(numreg).cast(IntegerType())) \
        .withColumn('estado', lit(0).cast(IntegerType()))

    df = df.withColumn('idpersonadetallado', row_number().over(Window.orderBy(monotonically_increasing_id()))+inc)

    return df



