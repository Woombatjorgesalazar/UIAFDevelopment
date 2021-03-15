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


    return df




def cruze_efectivo(hc, dfcruzar, dicccruzar, actual, actualsin):
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

    Returns
    -------
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Tabla resultado inicial a cargar a dwh_uiaf.

    """
    idsistema = dicccruzar['idsistema']
    idcatalogo = dicccruzar['idcatalogo']
    database = 'dwh_uiaf2'
    #dfdestino = 'dimproductosistema'
    namedfprincipal = 'dimproductodetallado'
    col_inc = 'idproductodetallado'
    adc = ''
    inc = cr.incremental(hc, database, namedfprincipal, col_inc)

    df, dfpt = cr.cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo, adc)


    df = df.withColumn('idcatalogo', lit(str(idcatalogo)).cast(IntegerType())) \
            .withColumn('idsistema', lit(str(idsistema)).cast(IntegerType()))  \
            .withColumn('idfechacargue', lit(str(actualsin)).cast(IntegerType())) \
            .withColumn('identregatransaccional', lit(0).cast(IntegerType())) \
            .withColumn('numeroregsistema', lit(0).cast(IntegerType())) \
            .withColumn('estado', lit(0).cast(IntegerType())) \
            .withColumn('fecha_transaccion', regexp_replace('fecha_transaccion', '-', '').cast(StringType())) \
            .withColumnRenamed('fecha_transaccion', 'idfechatransaccion') \
            .withColumn('numeroproductosistema', col('numero').cast(StringType())) \
            .withColumn('idsector', lit(0).cast(IntegerType())) \
            .withColumn('iddaneapertura', lit(0).cast(IntegerType())) \
            .withColumn('idfechaapertura', lit(0).cast(IntegerType())) \
            .withColumn('idestadoproducto', lit(0).cast(IntegerType())) \
            .withColumn('idproducto', lit('No homologado').cast(IntegerType())) \
            .withColumn('estado', lit(0).cast(IntegerType())) \
            
            
                
    
            
            
    df = df.withColumn('idproductodetallado', row_number().over(Window.orderBy(monotonically_increasing_id()))+inc)


    return df
