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
    
    cm.verbose_c(inc, 2)
    return int(inc)


######################Cruze de tablas sencillo#####################
def formateo_sent_sql(campos, adicionales):
    """
    Parameters
    ----------
    campos : TYPE: list
        DESCRIPTION: Lista con los campos a cruzar
    adicionales : TYPE: String
        DESCRIPTION: Cadena de consulta arreglada con los campos requeridos de la tabla a cruzar con la principal de hive.

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

    
    

    sent = """select t2.* {2}
            from tabla t1 right join dataset t2 on {0}
            where {1}""".format(cadena, cadena2, adicionales)
    
    cm.verbose_c(sent, 2)
    
    return sent



#################################Cruze en detalle###############################
def cruze_en_detalle(hc, dfcruzar, dicc, database, namedfprincipal, idsistema, idcatalogo, adc):
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
    adc : TYPE: String
        DESCRIPTION: Cadena de consulta arreglada con los campos requeridos de la tabla a cruzar con la principal de hive.

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
    sent = formateo_sent_sql(dicc['campos'], adc)
    
    df = hc.sql(sent)
    
    return df, dfpt