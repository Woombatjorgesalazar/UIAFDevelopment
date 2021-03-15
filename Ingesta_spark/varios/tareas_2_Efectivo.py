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


def formateo_sent_sql(campos, adicionales):
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

    sent = """select t1.*{1} 
            from tabla t1 inner join dataset t2 on {0}""".format(cadena, adicionales)
    print(sent)
    return sent

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

    
    
    sent = """select t1.*, t2.idfechatransaccion, t2.idfechacargue
            from tabla t1 inner join dataset t2 on {0}""".format(cadena)
    #print(sent)
    #cm.verbose_c(sent, 2)
    #exit(0)

    return sent


def cruze_en_detalle(hc, dfcruzar, dicc, database, namedfprincipal, idsistema, idcatalogo, adc):
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
        sent = formateo_sent_sql(dicc['campos'], adc)
    df = hc.sql(sent)
    
    return df, dfpt


def cruze_dane_sistema(hc, dfcruzar, dicccruzar):
    
    idsistema = dicccruzar['idsistema']
    idcatalogo = dicccruzar['idcatalogo']
    database = 'dwh_uiaf'
    namedfprincipal = 'dimdane'
    dfdestino = 'dimdanesistema'
    col_inc = 'iddanesistema'
    adc = ''
    inc = incremental(hc, database, dfdestino, col_inc)
    df,dfpt = cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo, adc)
    
    df = df.withColumn('nombremunicipiosistema', lit(None).cast(StringType())) \
        .withColumn('nombredetapartamentosistema', lit(None).cast(StringType())) \
        .withColumn('idsistema', lit(int(idsistema)).cast(IntegerType())) \
        .withColumn('idcatalogo', lit(int(idcatalogo)).cast(IntegerType())) \
        .withColumn('codigodanesistema', col('iddane'))
    df = df.withColumn('iddanesistema', row_number().over(Window.orderBy(monotonically_increasing_id()))+inc)
    
    return df
    
    
def cruze_moneda_sistema(hc, dfcruzar, dicccruzar, actual):
    
    idsistema = dicccruzar['idsistema']
    idcatalogo = dicccruzar['idcatalogo']
    database = 'dwh_uiaf'
    namedfprincipal = 'dimmoneda'
    dfdestino = 'dimmonedasistema'
    col_inc = 'idmonedasistema'
    adc = ''
    inc = incremental(hc, database, dfdestino, col_inc)
    df,dfpt = cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo, adc)
    
    df = df.withColumn('idcatalogo', lit(int(dicccruzar['idcatalogo'])).cast(IntegerType())) \
        .withColumn('idsistema',lit(int(dicccruzar['idsistema'])).cast(IntegerType())) \
        .withColumn('codigosistema', col('codigo').cast(StringType())) \
        .withColumn('monedasistema', lit(None).cast(StringType()))
    df = df.withColumn('idmonedasistema', row_number().over(Window.orderBy(monotonically_increasing_id()))+inc)
    
    #df.show()
    #df.printSchema()
    return df
    
def cruze_tipo_producto_sistema(hc, dfcruzar, dicccruzar, actual, actualsin):
    
    idsistema = dicccruzar['idsistema']
    idcatalogo = dicccruzar['idcatalogo']
    database = 'dwh_uiaf'
    dfdestino = 'dimtipoproductosistema'
    namedfprincipal = 'dimtipoproducto'
    col_inc = 'idtipoproductosistema'
    adc = ''
    inc = incremental(hc, database, dfdestino, col_inc)
    
    df, dfpt = cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo, adc)
    
    df = df.withColumn('idfechacargue', lit(actualsin.replace('-','')).cast(IntegerType())) \
            .withColumn('identregatransaccional', lit(0).cast(IntegerType())) \
            .withColumn('numeroregsistema', lit(0).cast(IntegerType())) \
            .withColumn('estado', lit(0).cast(IntegerType())) \
            .withColumn('idfechasistema', lit(0).cast(IntegerType())) \
            .withColumn('tipoproductosistema', col('tipoproducto').cast(StringType()))
    
    df = df.withColumn('idtipoproductosistema', row_number().over(Window.orderBy(monotonically_increasing_id()))+inc)
    #df.show()
    #df.printSchema()
    return df
    
    
def cruze_producto_sistema(hc, dfcruzar, dicccruzar, actual, actualsin):
        
    idsistema = dicccruzar['idsistema']
    idcatalogo = dicccruzar['idcatalogo']
    database = 'dwh_uiaf'
    dfdestino = 'dimproductosistema'
    namedfprincipal = 'dimproducto'
    col_inc = 'idproductosistema'
    adc = ', t2.fecha_transaccion'
    inc = incremental(hc, database, dfdestino, col_inc)
    
    df, dfpt = cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo, adc)
    
    df = df.withColumn('idpersona', lit(0).cast(IntegerType())) \
            .withColumn('idcatalogo', lit(str(idcatalogo)).cast(IntegerType())) \
            .withColumn('idsistema', lit(str(idsistema)).cast(IntegerType()))  \
            .withColumn('idfechacargue', lit(str(actualsin)).cast(IntegerType())) \
            .withColumn('identregatransaccional', lit(0).cast(IntegerType())) \
            .withColumn('numeroregsistema', lit(0).cast(IntegerType())) \
            .withColumn('estado', lit(0).cast(IntegerType())) \
            .withColumn('fecha_transaccion', regexp_replace('fecha_transaccion', '-', '').cast(StringType())) \
            .withColumnRenamed('fecha_transaccion', 'idfechatransaccion') \
            .withColumn('numeroproductosistema', col('numero').cast(StringType()))
            
                
    
            
            
    df = df.withColumn('idproductosistema', row_number().over(Window.orderBy(monotonically_increasing_id()))+inc)
    
    return df
    
def numero_registros(hc, encabezado, idcargue):

    sent = "select cantidad_registros from stage_uiaf.{0} \
                 where idcargue = {1}".format(encabezado, idcargue)
    

    df = hc.sql(sent)
    dfp = df.toPandas()
    try:
        num = int(dfp['cantidad_registros'][0])
    except IndexError:
        num = None
    return num

def cruze_persona_sistema(hc, dfcruzar, dicccruzar, actual, actualsin, idcargue):

    idsistema = dicccruzar['idsistema']
    idcatalogo = dicccruzar['idcatalogo']
    database = 'dwh_uiaf'
    dfdestino = 'dimpersonasistema'
    namedfprincipal = 'dimpersona'
    col_inc = 'idpersonasistema'
    adc = ''
    encabezado = 'te10_encabezado'
    inc = incremental(hc, database, dfdestino, col_inc)


    numreg = numero_registros(hc, encabezado, idcargue)
    

    df, dfpt = cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo, adc)


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

    df = df.withColumn('idpersonasistema', row_number().over(Window.orderBy(monotonically_increasing_id()))+inc)

    #df.show()
    #df.printSchema()
    #exit(0)

    return df








    
