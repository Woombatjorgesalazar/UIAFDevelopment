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

#actual = time.strftime("20%y-%m-%d %H:%M:%S")


#######################Contextos de spark y hive################
#sc = SparkContext()
#hc = HiveContext(sc)

def incremental(hc, database, dfdestino, col_inc):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    database : TYPE: string
        DESCRIPTION: Base de datos a la cual se le calcula el incremental.
    dfdestino : TYPE: string
        DESCRIPTION: Tabla de hive a la cual se le requiere calcular el incremental.
    col_inc : TYPE: string
        DESCRIPTION: Columna de la tabla a la cual se le quiere calcular el incremental.

    Returns
    -------
    inc : TYPE: int
        DESCRIPTION: Incremental de la tabla seleccionada.

    """
    sent = """select * from {0}.{1} 
    where {2} in (select max({3}) 
                               from {4}.{5})""".format(database, dfdestino, col_inc, col_inc, database, dfdestino)
    d1 = hc.sql(sent)
    dfpandas = d1.toPandas()
    try:
        inc = dfpandas[col_inc][0]
    except:
        inc = 0
    
    #print(inc,'Este es el incremental###############################')
    return int(inc)
    
def formateo_sent_sql(campos):
    """
    Parameters
    ----------
    campos : TYPE: list
        DESCRIPTION: Lista con los campos a cruzar.

    Returns
    -------
    sent : TYPE: string
        DESCRIPTION: Sentencia SQL autocreada para realizar el cruze.

    """
    cad = 'trim(t1.{0}) = trim(t2.{1})'
    cad2 = 't1.{0} is null'
    cadena = ''
    cadena2 = ''
    for index,i in enumerate(campos):
        if index == 0:
            cadena += '('
            cadena += cad.format(i, i)
            cadena2 += cad2.format(i)
            if i == campos[-1]:
                cadena += ')'
        elif i == campos[-1]:
            cadena += ' and '
            cadena += cad.format(i, i)
            cadena += ')'
            cadena2 += ' and '
            cadena2 += cad2.format(i)
        else:
            cadena += ' and '
            cadena += cad.format(i, i)
            cadena2 += ' and '
            cadena2 += cad2.format(i)

    
    print(cadena)

    sent = """select t1.* 
            from tabla t1 inner join dataset t2 on {0}""".format(cadena)
    print(sent)
    return sent
#campos = ['tipoidentificacion']
#formateo_sent_sql(campos)
def formateo_sent_sql_persona(campos):
    """
    Parameters
    ----------
    campos : TYPE: list
        DESCRIPTION: Lista con los campos a cruzar

    Returns
    -------
    sent : TYPE: string
        DESCRIPTION: Sentencia SQL autocreada para realizar el cruce.

    """
    cad = 'trim(t1.{0}) = trim(t2.{1})'
    cad2 = 't1.{0} is null'
    cadena = ''
    cadena2 = ''
    for index,i in enumerate(campos):
        if index == 0:
            cadena += '('
            cadena += cad.format(i, i)
            cadena2 += cad2.format(i)
            if i == campos[-1]:
                cadena += ')'
        elif i == campos[-1]:
            cadena += ' and '
            cadena += cad.format(i, i)
            cadena += ')'
            cadena2 += ' and '
            cadena2 += cad2.format(i)
        else:
            cadena += ' and '
            cadena += cad.format(i, i)
            cadena2 += ' and '
            cadena2 += cad2.format(i)

    
    print(cadena)

    sent = """select t1.*, t2.iddanevive, t2.idfechatransaccion
            from tabla t1 inner join dataset t2 on {0}""".format(cadena)
    
    return sent

def cruze_en_detalle(hc, dfcruzar, dicc, database, namedfprincipal, idsistema, idcatalogo):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dfcruzar : TYPE: Dataframe de pyspark
        DESCRIPTION: Dataframe que se preproceso para su posterior cruze.
    dicc : TYPE: dict
        DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.
    database : TYPE: string
        DESCRIPTION: Base de datos destino de las tablas dimsistema.
    namedfprincipal : TYPE: string
        DESCRIPTION: Tabla destino de la carga a realizar.
    idsistema : TYPE: string
        DESCRIPTION: Ide del sistema del cual se va a realizar la carga.
    idcatalogo : TYPE: string
        DESCRIPTION: Id del catalogo o la tabla a cargar.

    Returns
    -------
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Resultado del cruze.
    dfpt : TYPE: Dataframe de pandas
        DEESCRIPTION: Dataframe con las columnas de la tabla a cruzar.

    """
    ##################dfprincipal##########################
    sent = "select * from {0}.{1}" \
        .format(database, namedfprincipal)
    
    dfprincipal = hc.sql(sent)
    
    ##########################Buscar si la tabla es dimension conformada###########################
    consulta = "(select idtipocatalogo from tcatalogo t where nombrecatalogo = '{0}') as consult".format(namedfprincipal)
    df = cm.get_Query_Postgresql(hc, consulta)
    dfp = df.toPandas()
    idtipocatalogo = str(dfp['idtipocatalogo'][0])
    print('Este es el tipo de catalogo', idtipocatalogo)
    if idtipocatalogo != '4':
        
        return "El dataframeprincipal no es dimension conformada por lo cual no se puede aplicar el cruze en detalle"
    
    ####################Tabla de columnas en pandas de dataframe###################################
    consulta = "(select * from consult_columnas('{0}', 'tabla')) as consult".format(namedfprincipal)
    df1 = cm.get_Query_Postgresql(hc, consulta)
    dfpt = df1.toPandas()
    print(dfpt)
    
    ####################El dataframeprincipal si es dimension conformada############################
    
    ###############################Cruze###########################################################
    
    dfprincipal.registerTempTable('tabla')
    dfcruzar.registerTempTable('dataset')
    
    ##############################Cruze de tablas##################################################
    if namedfprincipal == 'dimpersona':
        sent = formateo_sent_sql_persona(dicc['campos'])
    else:
        sent = formateo_sent_sql(dicc['campos'])
    df = hc.sql(sent)
    
    return df, dfpt


def cruze_tipo_identificacion_sistema(hc, dfcruzar, dicccruzar):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dfcruzar : TYPE: Dataframe de pyspark
        DESCRIPTION:Dataset a cruzar con la tabla principal de hive.
    dicccruzar : TYPE: dict
        DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.

    Returns
    -------
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Resultado de dataframe cruzado.

    """
    idsistema = dicccruzar['idsistema']
    idcatalogo = dicccruzar['idcatalogo']
    database = 'dwh_uiaf'
    namedfprincipal = 'dimtipoidentificacion'
    dfdestino = 'dimtipoidentificacionsistema'
    col_inc = 'idtipoidentificacionsistema'
    inc = incremental(hc, database, dfdestino, col_inc)
    df, dfpt = cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo)
    
    
    
    df = df.withColumn('tipoidentificacionsistema', col('tipoidentificacion')) \
    .withColumn('descripcionsistema', lit(None).cast(StringType())) \
    .withColumn('idcatalogo', lit(int(idcatalogo)).cast(IntegerType())) \
    .withColumn('idsistema', lit(int(idsistema)).cast(IntegerType())) \
    .withColumn('observaciones', lit('Carga semiautomatica').cast(StringType()))
    ##########################ID sistema##################################################
    df = df.withColumn('idtipoidentificacionsistema', row_number().over(Window.orderBy(monotonically_increasing_id())) + inc)
    
    #df.printSchema()
    #df.show()
    #df.write.format("Hive").saveAsTable("%s.%s" % (database, dfdestino), mode="append")
    return df
    

"""
##############################Ensayo#########################
dicc = {'idsistema':'3',
 'idcatalogo':'7',
 'campos': ['tipoidentificacion']
 }
####################################Extraer dfcruzar###########################
sent = "select * from stage_uiaf.adres_bduda"
dfcruzar = hc.sql(sent).groupBy('segnomafi').count()
##############################Adaptar cruze con columnas iguales###############################
dfcruzar = dfcruzar.select(col("segnomafi").alias("tipoidentificacion"))
#dfcruzar.registerTempTable("dfcruzar")
cruze_tipo_identificacion_sistema(hc, dfcruzar, dicc)
"""



def cruze_dane_sistema(hc, dfcruzar, dicccruzar):
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
    database = 'dwh_uiaf'
    namedfprincipal = 'dimdane'
    dfdestino = 'dimdanesistema'
    df, dfpt = cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo)
    col_inc = 'iddanesistema'
    inc = incremental(hc, database, dfdestino, col_inc)
    #####################################Procedimiento###################
    
    df = df.withColumn('nombremunicipiosistema', lit(None).cast(StringType())) \
        .withColumn('nombredetapartamentosistema', lit(None).cast(StringType())) \
        .withColumn('idsistema', lit(int(idsistema)).cast(IntegerType())) \
        .withColumn('idcatalogo', lit(int(idcatalogo)).cast(IntegerType())) \
        .withColumn('codigodanesistema', col('iddane'))
    df = df.withColumn('iddanesistema', row_number().over(Window.orderBy(monotonically_increasing_id()))+inc)
    #df.printSchema()
    #df.show()
    #df.write.format("Hive").saveAsTable("%s.%s" % (database, dfdestino), mode="append")
    return df

##############################Ejemplo cruze dimdanesistema##################
"""
dicc = {'idsistema':'3',
 'idcatalogo':'7',
 'campos': ['iddane']
 }
#############Seleccion y ajuste de acuerdo a la tabla de stage_uiaf################
sent = "select * from stage_uiaf.adres_bduda"
dfcruzar = hc.sql(sent)
dfcruzar.registerTempTable("prueba")
dfcruzar = hc.sql("select concat(if(length(nivelsisben) = 1, concat('0',nivelsisben), nivelsisben), coddepafi) as iddane from prueba")
dfcruzar = dfcruzar.groupBy('iddane').count()
##############################Adaptar cruze con columnas iguales###############################
#dfcruzar = dfcruzar.select(col("").alias("nombremunicipiosistema"))
############################Obtencion de dataframe cruzado###########################
cruze_dane_sistema(hc, dfcruzar, dicc)
"""
#########################Tabla dimpaisessistema###############################
def cruze_paises_sistema(hc, dicc, actual):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dicc : TYPE: dict
        DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.
    actual : TYPE: string
        DESCRIPTION: Fecha actual para realizar el cargue.

    Returns
    -------
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Resultado de dataframe cruzado.

    """
    database = 'dwh_uiaf'
    dfdestino = 'dimpaisessistema'
    sent = "select * from dwh_uiaf.dimpais where nombre = 'Colombia'"
    col_inc = 'idpaisessistema'
    inc = incremental(hc, database, dfdestino, col_inc)
    df = hc.sql(sent)
    df = df.withColumn('idcatalogo',lit(int(dicc['idcatalogo'])).cast(IntegerType())) \
        .withColumn('idsistema',lit(int(dicc['idsistema'])).cast(IntegerType())) \
        .withColumn('nombresistema',lit(None).cast(StringType())) \
        .withColumn('moneda',lit('COP').cast(StringType())) \
        .withColumn('fechacarga',lit(actual).cast(StringType())) \
        .withColumn('codigopaissistema',lit(None).cast(StringType())) \
        .withColumn('observacion',lit('Este se esta cargando manual').cast(StringType())) \
        .drop('fechamodificacion')
    df = df.withColumn('idpaisessistema', row_number().over(Window.orderBy(monotonically_increasing_id()))+inc)
    
    #df.show()
    #df.printSchema()
    #df.write.format("Hive").saveAsTable("%s.%s" % (database, dfdestino), mode="append")
    return df

#########################Tabla dimmonedasistema###############################
def cruze_moneda_sistema(hc, dicc, actual):
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
    database = 'dwh_uiaf'
    dfdestino = 'dimmonedasistema'
    sent = "select * from dwh_uiaf.dimmoneda where codigo = 'COP'"
    col_inc = 'idmonedasistema'
    inc = incremental(hc, database, dfdestino, col_inc)
    df = hc.sql(sent)
    
    df = df.withColumn('idcatalogo', lit(int(dicc['idcatalogo'])).cast(IntegerType())) \
        .withColumn('idsistema',lit(int(dicc['idsistema'])).cast(IntegerType())) \
        .withColumn('codigosistema',lit(None).cast(StringType())) \
        .withColumn('monedasistema', lit(None).cast(StringType()))
    df = df.withColumn('idmonedasistema', row_number().over(Window.orderBy(monotonically_increasing_id()))+inc)
    
    #df.show()
    #df.printSchema()
    #df.write.format("Hive").saveAsTable("%s.%s" % (database, dfdestino), mode="append")
    return df
#####################Tabla dimactividadeconomicasistema#######################
def cruze_actividad_sistema(hc, dicc, actual):
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
    dfdestino = 'dimactividadeconomicadetallado'
    col_inc = 'idactividadeconomicadetallado'
    inc = incremental(hc, database, dfdestino, col_inc)
    sent = "select * from dwh_uiaf.dimactividadeconimica where codigoactividad = 'Nore'"
    df = hc.sql(sent)
    df = df.withColumn('idcatalogo', lit(int(dicc['idcatalogo'])).cast(IntegerType())) \
        .withColumn('idsistema', lit(int(dicc['idsistema'])).cast(IntegerType())) \
        .withColumn('actividadeconomicasistema', lit(None).cast(StringType())) \
        .withColumn('codigoactividadsistema', lit(None).cast(StringType()))
    df = df.withColumn('idactividadeconomicadetallado', row_number().over(Window.orderBy(monotonically_increasing_id()))+inc)
    
    #df.show()
    #df.printSchema()
    #df.write.format("Hive").saveAsTable("%s.%s" % (database, dfdestino), mode="append")    
    return df
    

####################tabla dimpersonasistema###################################
def cruze_persona_sistema(hc, dfcruzar, dicc, actual, actualsin):
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

    Returns
    -------
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Resultado de dataframe cruzado.

    """
    idsistema = dicc['idsistema']
    idcatalogo = dicc['idcatalogo']
    database = 'dwh_uiaf'
    dfdestino = 'dimpersonasistema'
    namedfprincipal = 'dimpersona'
    col_inc = 'idpersonasistema'
    ##############Incremental##############
    inc = incremental(hc, database, dfdestino, col_inc)
    #################Cruze####################
    df, dfpt = cruze_en_detalle(hc, dfcruzar, dicc, database, namedfprincipal, idsistema, idcatalogo)
    
    df = df.withColumn('idpersonasistema', row_number().over(Window.orderBy(monotonically_increasing_id()))+inc)
    
    df = df.withColumn('idtipoidentificacionsistema', col('idtipoidentificacion').cast(StringType())) \
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
        .withColumn('identregasistema', lit(1).cast(IntegerType())) \
        .withColumn('numeroregsistema', lit(1).cast(IntegerType())) \
        .withColumn('estado', lit(1).cast(IntegerType()))
    
    df = df.drop('iddaneresidencia')
    df = df.withColumn('fechanacimientocreacion', col('fechanacimientocreacion').cast(TimestampType()))
    #df.show()
    #df.printSchema()
    
    #df.write.format("Hive").saveAsTable("%s.%s" % (database, dfdestino), mode="append")
    return df



