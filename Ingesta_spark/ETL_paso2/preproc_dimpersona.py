from pyspark import SparkContext, HiveContext, SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
from datetime import datetime,date
import common as cm
import utils
from pyspark.sql.functions import *
import time

import gestioncargue





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
    if sigla == 'BDUDA':
        dfcruzar = preproc_adres(hc, dicc)


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
    idcargue = 157
    sent = """
    select coalesce(t1.tipo_identificacion_titular, t2.tipo_identificacion_realiza) as idtipoidentificacion,
        coalesce(t1.numero_identificacion_titular, t2.numero_identificacion_realiza) as identificacion,
        coalesce(t1.primer_apellido_titular, t2.primer_apellido_realiza) as nombresrazonsocial,
        coalesce(t1.codigo_departamento_municipio, t2.codigo_departamento_municipio) as codigodanetransaccion,
        if(t1.tipo_identificacion_titular > 0, 'recibe', 'realiza') as rol,
        coalesce(t1.fecha_cargue, t2.fecha_cargue) as fechacargue,
        coalesce(t1.fecha_transaccion, t2.fecha_transaccion) as fecha_transaccion,
        coalesce(t1.idcargue, t2.idcargue) as idcargue
       
    from stage_uiaf.te10_detalles as t1
    full outer join (select tipo_identificacion_realiza, numero_identificacion_realiza,
                                primer_apellido_realiza, codigo_departamento_municipio, fecha_cargue,
                                fecha_transaccion, idcargue
                        from stage_uiaf.te10_detalles) as t2
    on t1.tipo_identificacion_titular = t2.tipo_identificacion_realiza
    """
    dfcruzar = hc.sql(sent)
    
    dfcruzar.registerTempTable('p1')

    sent = """select p1.idtipoidentificacion, p1.identificacion, p1.nombresrazonsocial,
                p2.idrol as idroluiaf, cast(replace(cast(cast(p1.fechacargue as date) as string), '-', '') as int) as idfechacargue,
                 cast(replace(cast(p1.fecha_transaccion as string), '-', '') as int) as idfechatransaccion
            from p1 inner join dwh_uiaf.dimrol p2 on p1.rol = p2.rol
            where p1.idcargue = {0}""".format(str(idcargue))

    
    dfcruzar = hc.sql(sent)
    
    #dfcruzar.show()
    #dfcruzar.printSchema()
    #exit(0)
    
    return dfcruzar

def preproc_adres(hc, dicc):
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
        DESCRIPTION: Dataframe preprocesado manualmente para su posterior cruze.

    """
    idcargue = utils.last_idcargue(hc, 'BDUDA')
    #idcargue = 152
    ############################Condicion################################
    
    sent = "select * from stage_uiaf.adres_bduda"
    dfcruzar = hc.sql(sent)
    dfcruzar.registerTempTable("prueba")
    dfcruzar = hc.sql("select *, concat(if(length(nivelsisben) = 1, \
                      concat('0',nivelsisben), nivelsisben), \
        right(concat('000', coddepafi), 3)) as iddanevive, \
        concat(priapeafi,' ', segapeafi, ' ', prinomafi) as nombresrazonsocial \
        from prueba where idcargue = {0}".format(str(idcargue)))
    
    #dfcruzar.show()
    
    dfcruzar.registerTempTable("prueba")
    
    dfcruzar = hc.sql("""select from_unixtime(unix_timestamp(t1.fecnacafi, 'dd/MM/yyyy'), 'yyyy-MM-dd') as fechanacimientocreacion, 
                      t1.tipdocafi as identificacion, 
                      t2.idtipoidentificacion, t1.iddanevive, t1.nombresrazonsocial, 
                      from_unixtime(unix_timestamp(t1.tipafi, 'dd/MM/yyyy'), 'yyyy-MM-dd') as fechatransaccion
                      from prueba t1 inner join dwh_uiaf.dimtipoidentificacion t2 
                      on trim(t1.segnomafi) = trim(t2.tipoidentificacion)""")
    
    dfcruzar.registerTempTable("prueba")
    dfcruzar = hc.sql("select t1.*,t2.idfecha as idfechatransaccion from prueba t1 \
                      inner join dwh_uiaf.dimfecha t2 \
                      on trim(t1.fechatransaccion) = trim(t2.fecha)")
    dfcruzar = dfcruzar.drop('fechatransaccion')
    
    
    return dfcruzar














