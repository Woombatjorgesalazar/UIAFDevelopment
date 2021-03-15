from pyspark import SparkContext, HiveContext, SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql.functions import udf
from pyspark.sql.window import Window
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

def clasificador(hc, dicc, sigla):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dicc : TYPE: dict
        DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.
    sigla : TYPE: String
        DESCRIPTION: Palabra clave acerca de el origen de las tablas a cargar.

    Returns
    -------
    dfcruzar : TYPE: Dataframe de pyspark
        DESCRIPTION: Tabla a cruzar preprocesada de stage_uiaf.

    """
    if sigla == 'TE10':
        dfcruzar = preproc_efectivo(hc, dicc)


    return dfcruzar


def preproc_efectivo(hc, dicc):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dicc : TYPE: dict
        DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.
    

    Returns
    -------
    dfcruzar : TYPE: Dataframe de pyspark
        DESCRIPTION: Tabla a cruzar preprocesada de stage_uiaf.

    """
    idcargue = utils.last_idcargue(hc, 'TE10')

    sent = """select t2.idpersona as idpersonatitular, t3.iddanesistema as iddanetransaccion, t4.idtipotransaccion,
    t6.identidad as identidadtransaccion, t7.idsector as idsectortransaccion, t8.idpersona as idpersonacontraparte,
    cast(replace(t1.fecha_transaccion, '-', '') as integer) as idfecha, 
    t9.idproducto, cast(replace(cast(cast(t1.fecha_cargue as date) as string), '-', '') as int) as idfechacargue,
    cast(t5.cantidad_registros as int) as numeroregsistema, 
    cast(t1.valor_transaccion as float) as valor,
    cast(replace(t1.fecha_transaccion, '-', '') as int) as idfechatransaccion,
    t10.idmoneda
    from stage_uiaf.te10_detalles as t1
    inner join dwh_uiaf.dimpersonasistema as t2
    on trim(t1.numero_identificacion_titular) = trim(t2.identificacionsistema)
    inner join dwh_uiaf.dimdanesistema as t3
    on trim(if(length(cast(t1.codigo_departamento_municipio as string)) < 5, concat('0', 
    cast(t1.codigo_departamento_municipio as string)), cast(t1.codigo_departamento_municipio as string))) = trim(t3.iddanesistema)
    inner join dwh_uiaf.dimtipotransaccionsistema as t4
    on t1.tipo_transaccion = t4.idtipotransaccionsistema
    inner join stage_uiaf.te10_encabezado as t5
    on t1.idcargue = t5.idcargue
    inner join dwh_uiaf.dimentidadsistema as t6
    on t5.codigo_entidad = t6.identificacionentidadsistema
    inner join dwh_uiaf.dimsectorsistema as t7
    on t5.sector_entidad = t7.idsectorsistema
    inner join dwh_uiaf.dimpersonasistema as t8
    on trim(t1.numero_identificacion_realiza) = trim(t8.identificacionsistema)
    inner join dwh_uiaf.dimproductosistema as t9
    on trim(t1.numero_cuenta) = trim(t9.numeroproductosistema)
    inner join dwh_uiaf.dimmonedasistema as t10
    on trim(t1.tipo_moneda) = trim(t10.codigosistema)
    where t1.idcargue = {0}
    """.format(str(idcargue))
    dfcruzar = hc.sql(sent)
    

    database = 'dwh_uiaf2'
    dfdestino = 'factefectivo'
    col_inc = 'idefectivo'
    idsistema = dicc['idsistema']
    idcatalogo = dicc['idcatalogo']
    inc = incremental(hc, database, dfdestino, col_inc)

    dfcruzar = dfcruzar.withColumn('idefectivo', row_number().over(Window.orderBy(monotonically_increasing_id()))+inc) \
                .withColumn('idcatalogo', lit(str(idcatalogo)).cast(IntegerType())) \
                .withColumn('idsistema', lit(str(idsistema)).cast(IntegerType())) \
                .withColumn('estado', lit(str(0)).cast(IntegerType())) \
                .withColumn('identregasistema', lit(str(0)).cast(IntegerType()))
    
    return dfcruzar











