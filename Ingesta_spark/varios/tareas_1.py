from pyspark import SparkContext, HiveContext, SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql.functions import desc, row_number, monotonically_increasing_id
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
import common as cm
import utils
from pyspark.sql.functions import *


#######################Contextos de spark y hive################
#sc = SparkContext()
#hc = HiveContext(sc)

#########################Creando test de prueba#####################
#dfprincipal = hc.sql('select * from dwh_uiaf.dimpersona where dimpersona.idpersona != "1"')
#dfprincipal.show(10)

#dfcruzar = hc.sql('select * from dwh_uiaf.dimpersona where dimpersona.idpersona = "1" or dimpersona.idpersona = "2"')
#dfcruzar.show()

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


######################Cruze de tablas sencillo#####################
def formateo_sent_sql(campos):
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

    sent = """select t2.* 
            from tabla t1 right join dataset t2 on {0}
            where {1}""".format(cadena, cadena2)
    
    
    return sent
#campos = ['idpersona', 'iddane']
#formateo_sent_sql(campos)

def cruze_sencillo(hc, dfprincipal, dfcruzar, campos_cruzar):
        """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dfprincipal : TYPE: Dataframe de pyspark
        DESCRIPTION: Dataframe principal para realizar el cruce.
    dfcruzar : TYPE: Dataframe de pyspark
        DESCRIPTION: Dataframe secundario para realizar el cruce.
    campos_cruzar : TYPE: list
        DESCRIPTION: Lista con los campos que se realizara el cruce. 

    Returns
    -------
    None.

    """
        dfprincipal.registerTempTable('tabla1')
        dfcruzar.registerTempTable('tabla2')
        sent = formateo_sent_sql(campos_cruzar)
        print(sent)
        ####################Lanzador de consultas de hive###################
        hc.sql(sent).show(10)

def cruze_tablas(hc, tabla1, tabla2):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    tabla1 : TYPE: int
        DESCRIPTION. Id del catalogo a cruzar.
    tabla2 : TYPE: int
        DESCRIPTION. Ide del catalogo secundario a cruzar.

    Returns
    -------
    None.

    """
    tabla1 = str(tabla1)
    tabla2 = str(tabla2)
    if tabla1.isdigit():
        query = "(select nombrecatalogo from tcatalogo where idcatalogo = '{}') as consult".format(tabla1)
        df = cm.get_Query_Postgresql(hc, query)
        dfp = df.toPandas()
        tabla1 = str(dfp['nombrecatalogo'][0])
    if tabla2.isdigit():
        query = "(select nombrecatalogo from tcatalogo where idcatalogo = '{}') as consult".format(tabla2)
        df = cm.get_Query_Postgresql(hc, query)
        dfp = df.toPandas()
        tabla2 = str(dfp['nombrecatalogo'][0])
        
    consulta = "(select * from cruze_tablas('{}','{}')) as consult".format(tabla1,tabla2)
    dff = cm.get_Query_Postgresql(hc, consulta)
    dff.show()
    print(tabla1,tabla2, '######Estas son las tablas')
    

#cruze_tablas(hc, '71', '72')

############Lanzar funcion####################

#campos = ['idpersona', 'identificacion']
#cruze_sencillo(hc, dfprincipal, dfcruzar, campos)
#formateo_sent_sql(campos)

#################################Cruze en detalle###############################
def cruze_en_detalle(hc, dfcruzar, dicc, database, namedfprincipal, idsistema, idcatalogo):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dfcruzar : TYPE: Dataframe de pyspark.
        DESCRIPTION: Dataframe que se preproceso para su posterior cruze.
    dicc : TYPE: dict
        DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.
    database : TYPE: string
        DESCRIPTION: Base de datos destino de las tablas dimsistema.
    namedfprincipal : TYPE: string
        DESCRIPTION: Tabla destino de la carga a realizar.
    idsistema : TYPE: string
        DESCRIPTION: Id del sistema del cual se va a realizar la carga.
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
    
    sent = "select * from {0}.{1} where idsistema = {2} and idcatalogo = {3}" \
        .format(database, namedfprincipal, idsistema, idcatalogo)
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
    namedfprincipal = 'dimtipoidentificacionsistema'
    col_inc = 'idtipoidentificacionsistema'
    inc = incremental(hc, database, namedfprincipal, col_inc)
    df, dfpt = cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo)
    
    #####################################Procedimiento###################
    colminc = namedfprincipal.replace('dim', '')
    
    #df = df.withColumn('tipoidentificacionsistema', col('tipoidentificacion').cast(StringType()))
    
    for index,i in enumerate(dfpt['nombre_columna']):
        
        if i.lower() == ('id'+ colminc.replace('sistema', '')):
            df = df.withColumn(i, lit(2).cast(IntegerType()))
        elif i.lower() == (colminc.replace('sistema', '')):
            df = df.withColumn(i, lit('Noho').cast(StringType()))
    
    df = df.withColumn('descripcionidentificacion', lit('No homologado').cast(StringType())) \
        .withColumn('descripcionsistema', lit('').cast(StringType())) \
        .withColumn('idcatalogo', lit(int(idcatalogo)).cast(IntegerType())) \
        .withColumn('idsistema', lit(int(idsistema)).cast(IntegerType())) \
        .withColumn('observaciones', lit('').cast(StringType()))
        
    df = df.withColumn(col_inc, row_number().over(Window.orderBy(monotonically_increasing_id())) + inc)
    
    #df.show()
    #df.printSchema()
    return df

"""
################Ejemplo para dimtipoidentificacionsistema###########################
dicc = {'idsistema':'3',
 'idcatalogo':'7',
 'campos': ['tipoidentificacionsistema']
 }
sent = "select * from stage_uiaf.adres_bduda"
dfcruzar = hc.sql(sent).groupBy('segnomafi').count()
##############################Adaptar cruze con columnas iguales###############################
dfcruzar = dfcruzar.select(col("segnomafi").alias("tipoidentificacionsistema"))
dffinal = cruze_tipo_identificacion_sistema(hc, dfcruzar, dicc)
dffinal.show()
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
    namedfprincipal = 'dimdanesistema'
    col_inc = 'iddanesistema'
    inc = incremental(hc, database, namedfprincipal, col_inc)
    df, dfpt = cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo)
    
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

"""
##############################Ejemplo cruze dimdanesistema##################
dicc = {'idsistema':'3',
 'idcatalogo':'7',
 'campos': ['codigodanesistema']
 }
#############Seleccion y ajuste de acuerdo a la tabla de stage_uiaf################
sent = "select * from stage_uiaf.adres_bduda"
dfcruzar = hc.sql(sent).groupBy('segnomafi').count()
##############################Adaptar cruze con columnas iguales###############################
dfcruzar = dfcruzar.select(col("nombrecolumnascruze").alias("nombremunicipiosistema"))
############################Obtencion de dataframe cruzado###########################
cruze_dane_sistema(hc, dfcruzar, dicc)
"""  

def cruze_paises_sistema(hc, dfcruzar, dicccruzar, actual):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dfcruzar : TYPE: Dataframe de pyspark
        DESCRIPTION: Dataset a cruzar con la tabla principal de hive.
    dicccruzar : TYPE: dict
        DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.
    actual : TYPE: string
        DESCRIPTION: Fecha actual de carga.

    Returns
    -------
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Resultado de dataframe cruzado.

    """
    idsistema = dicccruzar['idsistema']
    idcatalogo = dicccruzar['idcatalogo']
    database = 'dwh_uiaf'
    namedfprincipal = 'dimpaisessistema'
    col_inc = 'idpaisessistema'
    inc = incremental(hc, database, namedfprincipal, col_inc)
    df, dfpt = cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo)
    
    #####################################Procedimiento###################
    colminc = namedfprincipal.replace('dim', '')
    """
    for index,i in enumerate(dfpt['nombre_columna']):
        
        if i.lower() == ('id'+ colminc.replace('sistema', '')):
            df = df.withColumn(i, lit(2).cast(IntegerType()))
    """
    
    df = df.withColumn('idpais', lit(2).cast(IntegerType())) \
        .withColumn('idmoneda', lit(2).cast(IntegerType())) \
        .withColumn('codigopais', lit('Noho').cast(StringType())) \
        .withColumn('nombre', lit('No homologado').cast(StringType())) \
        .withColumn('moneda', lit('COP').cast(StringType())) \
        .withColumn('fechacarga', lit(str(actual)).cast(StringType())) \
        .withColumn('idcatalogo', lit(int(idcatalogo)).cast(IntegerType())) \
        .withColumn('idsistema', lit(int(idsistema)).cast(IntegerType())) \
        .withColumn('observacion', lit('Cargue semimanual').cast(StringType())) \
        .withColumn('nombresistema', lit(None).cast(StringType())) \
        .withColumn('codigopaissistema', lit(None).cast(StringType()))
        
    df = df.withColumn(col_inc, row_number().over(Window.orderBy(monotonically_increasing_id())) + inc)
    
    return df
"""
##############################Ejemplo cruze dimpaisessistema##################
dicc = {'idsistema':'3',
 'idcatalogo':'7',
 'campos': ['codigopaissistema']
 }
#############Seleccion y ajuste de acuerdo a la tabla de stage_uiaf################
sent = "select * from stage_uiaf.adres_bduda"
dfcruzar = hc.sql(sent).groupBy('segnomafi').count()
##############################Adaptar cruze con columnas iguales###############################
dfcruzar = dfcruzar.select(col("nombrecolumnascruze").alias("nombremunicipiosistema"))
############################Obtencion de dataframe cruzado###########################
cruze_dane_sistema(hc, dfcruzar, dicc)
"""


def cruze_moneda_sistema(hc, dfcruzar, dicccruzar):
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
    namedfprincipal = 'dimmonedasistema'
    col_inc = 'idmonedasistema'
    inc = incremental(hc, database, namedfprincipal, col_inc)
    df, dfpt = cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo)
    
    #####################################Procedimiento###################
    colminc = namedfprincipal.replace('dim', '')
    """
    for index,i in enumerate(dfpt['nombre_columna']):
        
        if i.lower() == ('id'+ colminc.replace('sistema', '')):
            df = df.withColumn(i, lit(2).cast(IntegerType()))
    """
    
    df = df.withColumn('idmoneda', lit(2).cast(IntegerType())) \
        .withColumn('moneda', lit('No homologado').cast(StringType())) \
        .withColumn('codigo', lit('Noho').cast(StringType())) \
        .withColumn('idcatalogo', lit(int(idcatalogo)).cast(IntegerType())) \
        .withColumn('idsistema', lit(int(idsistema)).cast(IntegerType())) \
        .withColumn('monedasistema', lit(None).cast(StringType())) \
        .withColumn('codigosistema', lit(None).cast(StringType()))
        
    df = df.withColumn(col_inc, row_number().over(Window.orderBy(monotonically_increasing_id())) + inc)
    
    return df

"""
##############################Ejemplo cruze dimmonedasistema##################
dicc = {'idsistema':'3',
 'idcatalogo':'7',
 'campos': ['codigosistema']
 }
#############Seleccion y ajuste de acuerdo a la tabla de stage_uiaf################
sent = "select * from stage_uiaf.adres_bduda"
dfcruzar = hc.sql(sent).groupBy('segnomafi').count()
##############################Adaptar cruze con columnas iguales###############################
dfcruzar = dfcruzar.select(col("nombrecolumnascruze").alias("nombrecolumnasprincipal"))
############################Obtencion de dataframe cruzado###########################
cruze_moneda_sistema(hc, dfcruzar, dicc)
"""


def cruze_actividad_sistema(hc, dfcruzar, dicccruzar):
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
    namedfprincipal = 'dimactividadeconomicasistema'
    col_inc = 'idactividadeconomicasistema'
    inc = incremental(hc, database, namedfprincipal, col_inc)
    df, dfpt = cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo)
    
    #####################################Procedimiento###################
    colminc = namedfprincipal.replace('dim', '')
    """
    for index,i in enumerate(dfpt['nombre_columna']):
        
        if i.lower() == ('id'+ colminc.replace('sistema', '')):
            df = df.withColumn(i, lit(2).cast(IntegerType()))
    """
    
    df = df.withColumn('idactividadeconomica', lit(2).cast(IntegerType())) \
        .withColumn('actividadeconomica', lit('No homologado').cast(StringType())) \
        .withColumn('codigoactividad', lit('Noho').cast(StringType())) \
        .withColumn('idcatalogo', lit(int(idcatalogo)).cast(IntegerType())) \
        .withColumn('idsistema', lit(int(idsistema)).cast(IntegerType())) \
        .withColumn('actividadeconomicasistema', lit(None).cast(StringType())) \
        .withColumn('codigoactividadsistema', lit(None).cast(StringType()))
        
    df = df.withColumn(col_inc, row_number().over(Window.orderBy(monotonically_increasing_id())) + inc)
    
    return df

"""
##############################Ejemplo cruze dimactividadeconomicasistema##################
dicc = {'idsistema':'3',
 'idcatalogo':'7',
 'campos': ['codigoactividadsistema']
 }
#############Seleccion y ajuste de acuerdo a la tabla de stage_uiaf################
sent = "select * from stage_uiaf.adres_bduda"
dfcruzar = hc.sql(sent).groupBy('segnomafi').count()
##############################Adaptar cruze con columnas iguales###############################
dfcruzar = dfcruzar.select(col("nombrecolumnascruze").alias("nombremunicipiosistema"))
############################Obtencion de dataframe cruzado###########################
cruze_actividad_economica_sistema(hc, dfcruzar, dicc)
"""

###############Recordatorio no reporta actividadeconomica##########################


def cruze_persona_sistema(hc, dfcruzar, dicccruzar, actualsin):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dfcruzar : TYPE: Dataframe de pyspark
        DESCRIPTION: Dataset a cruzar con la tabla principal de hive.
    dicccruzar : TYPE: dict
        DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.
    actualsin : TYPE: string
        DESCRIPTION: fecha actual sin guiones

    Returns
    -------
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Resultado de dataframe cruzado.

    """
    idsistema = dicccruzar['idsistema']
    idcatalogo = dicccruzar['idcatalogo']
    database = 'dwh_uiaf'
    namedfprincipal = 'dimpersonasistema'
    col_inc = 'idpersonasistema'
    inc = incremental(hc, database, namedfprincipal, col_inc)
    df, dfpt = cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo)
    
    #####################################Procedimiento###################
    colminc = namedfprincipal.replace('dim', '')
    """
    for index,i in enumerate(dfpt['nombre_columna']):
        
        if i.lower() == ('id'+ colminc.replace('sistema', '')):
            df = df.withColumn(i, lit(2).cast(IntegerType()))
    """
    
    df = df.withColumn('idpersona', lit(2).cast(IntegerType())) \
        .withColumn('idtipoidentificacionsistema', col('idtipoidentificacion').cast(StringType())) \
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
        .withColumn('identregasistema', lit(1).cast(IntegerType())) \
        .withColumn('numeroregsistema', lit(1).cast(IntegerType())) \
        .withColumn('estado', lit(1).cast(IntegerType())) \
        .withColumn('idroluiaf', lit(11).cast(IntegerType()))
    
    df = df.withColumn(col_inc, row_number().over(Window.orderBy(monotonically_increasing_id())) + inc)
    
    #df.show()
    #df.printSchema()
    return df
    

"""
##############################Ejemplo cruze dimpersonasistema##################
dicc = {'idsistema':'3',
 'idcatalogo':'7',
 'campos': ['identificacionsistema']
 }
#############Seleccion y ajuste de acuerdo a la tabla de stage_uiaf################
sent = "select * from stage_uiaf.adres_bduda"
dfcruzar = hc.sql(sent).groupBy('segnomafi').count()
##############################Adaptar cruze con columnas iguales###############################
dfcruzar = dfcruzar.select(col("nombrecolumnascruze").alias("nombremunicipiosistema"))
############################Obtencion de dataframe cruzado###########################
cruze_personas_sistema(hc, dfcruzar, dicc)
"""




    
"""
archivo = "/home/woombatcg/Desktop/GitDesarrollo/Ingesta_spark/archivos/sfc/CE018-2019/prueba2/2020/03/Validos/query-impala-8940.csv"
enc=utils.detectar_codec(archivo)
archivo = "/user/woombatcg/documentos/query-impala-8940.csv"
dataset = hc.read.load(archivo, format="csv", sep=",", header=True,  charset=enc['encoding'])

data = dataset.groupBy('tipdocafi').count()
#cruze_en_detalle(hc, 'dimtipoidentificacionsistema', data, 'tipoidentificacion', 'tipdocafi', 'dwh_uiaf')
"""









