from pyspark import SparkContext, HiveContext, SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
import common as cm
import utils
from pyspark.sql.functions import *
import time
import tareas_2 as t2
import tareas_1 as t1





    
def preproc_dane(hc, dicc):
    sent = "select * from stage_uiaf.te10_detalles"
    dfcruzar = hc.sql(sent)
    
    
    dfcruzar.registerTempTable("prueba")
    sent = "select if(length(cast(codigo_departamento_municipio as string)) = 4, \
        concat('0', cast(codigo_departamento_municipio as string)), \
        cast(codigo_departamento_municipio as string)) as iddane from prueba"
    dfcruzar = hc.sql(sent)
    dfcruzar.groupBy('iddane').count()
    #dfcruzar.show()
    #dfcruzar.printSchema()
    #cm.verbose_c('Este es el final', 2)
    return dfcruzar

def preproc_moneda(hc, dicc):
    sent = "select * from stage_uiaf.te10_detalles"
    dfcruzar = hc.sql(sent)
    
    dfcruzar = dfcruzar.withColumn('codigo', col('tipo_moneda').cast(StringType()))
    dfcruzar = dfcruzar.groupBy('codigo').count()
    
    return dfcruzar
    
def preproc_tipo_producto(hc, dicc):
    sent = "select * from stage_uiaf.te10_detalles"
    dfcruzar = hc.sql(sent)
    
    dfcruzar = dfcruzar.withColumn('tipoproducto', col('tipo_producto').cast(StringType()))
    dfcruzar = dfcruzar.groupBy('tipoproducto').count()
    #dfcruzar.show()
    #dfcruzar.printSchema()
    
    return dfcruzar


def preproc_producto(hc, dicc, sigla):
    codent = preproc_producto_enc(hc, dicc, sigla)
    
    sent = "select * from stage_uiaf.te10_detalles"
    dfcruzar = hc.sql(sent)
    
    dfcruzar = dfcruzar.withColumn('numero', col('numero_cuenta').cast(StringType()))
        #.withColumn('identificacionentidad', lit(str(codent)).cast(StringType())) \

    #dfcruzar = dfcruzar.groupBy('numero').count()
    dfcruzar = dfcruzar.select(['numero','fecha_transaccion'])
    
    
    return dfcruzar

def preproc_persona(hc, dicc):
    
    sent = """
    select coalesce(t1.tipo_identificacion_titular, t2.tipo_identificacion_realiza) as idtipoidentificacion,
        coalesce(t1.numero_identificacion_titular, t2.numero_identificacion_realiza) as identificacion,
        coalesce(t1.primer_apellido_titular, t2.primer_apellido_realiza) as nombresrazonsocial,
        coalesce(t1.codigo_departamento_municipio, t2.codigo_departamento_municipio) as codigodanetransaccion,
        if(t1.tipo_identificacion_titular > 0, 'recibe', 'realiza') as rol,
        coalesce(t1.fecha_cargue, t2.fecha_cargue) as fechacargue,
        coalesce(t1.fecha_transaccion, t2.fecha_transaccion) as fecha_transaccion
       
    from stage_uiaf.te10_detalles as t1
    full outer join (select tipo_identificacion_realiza, numero_identificacion_realiza,
                                primer_apellido_realiza, codigo_departamento_municipio, fecha_cargue,
                                fecha_transaccion
                        from stage_uiaf.te10_detalles) as t2
    on t1.tipo_identificacion_titular = t2.tipo_identificacion_realiza
    """
    dfcruzar = hc.sql(sent)
    dfcruzar.registerTempTable('p1')

    sent = """select p1.idtipoidentificacion, p1.identificacion, p1.nombresrazonsocial,
                p2.idrol, cast(replace(cast(cast(p1.fechacargue as date) as string), '-', '') as int) as idfechacargue,
                 cast(replace(cast(p1.fecha_transaccion as string), '-', '') as int) as idfechatransaccion
            from p1 inner join dwh_uiaf.dimrol p2 on p1.rol = p2.rol"""

    
    dfcruzar = hc.sql(sent)
    
    #dfcruzar.show()
    #dfcruzar.printSchema()
    #exit(0)
    
    return dfcruzar


def preproc_factefectivo(hc, dicc):
    
    sent = """select t2.idpersona as idpersonatitular, t3.iddane as iddanetransaccion, t4.idtipotransaccion,
    t6.identidad as identidadtransaccion, t7.idsector, t8.idpersona as idpersonacontraparte,
    cast(replace(t1.fecha_transaccion, '-', '') as integer) as idfecha, 
    t9.idproducto, cast(replace(cast(cast(p1.fechacargue as date) as string), '-', '') as int) as idfechacargue,
    cast(t5.cantidad_registros as int) as numeroregsistema, 
    cast(t1.valor_transaccion as float) as valor,
    cast(replace(t1.fecha_transaccion, '-', '') as int) as idfechatransaccion,
    t10.idmoneda
    from stage_uiaf.te10_detalles as t1
    inner join dwh_uiaf.dimpersona as t2
    on trim(t1.numero_identificacion_titular) = trim(t2.identificacion)
    inner join dwh_uiaf.dimdane as t3
    on trim(if(length(cast(t1.codigo_departamento_municipio as string)) < 5, concat('0', 
    cast(t1.codigo_departamento_municipio as string)), cast(t1.codigo_departamento_municipio as string))) = trim(t3.iddane)
    inner join dwh_uiaf.dimtipotransaccion as t4
    on t1.tipo_transaccion = t4.idtipotransaccion
    inner join stage_uiaf.te10_encabezado as t5
    on t1.idcargue = t5.idcargue
    inner join dwh_uiaf.dimentidad as t6
    on t5.codigo_entidad = t6.identificacionentidad
    inner join dwh_uiaf.dimsector as t7
    on t5.sector_entidad = t7.idsector
    inner join dwh_uiaf.dimpersona as t8
    on trim(t1.numero_identificacion_realiza) = trim(t8.identificacion)
    inner join dwh_uiaf.dimproducto as t9
    on trim(t1.numero_cuenta) = trim(t9.numero)
    inner join dwh_uiaf.dimmoneda as t10
    on trim(t1.tipo_moneda) = trim(t10.codigo)
    """
    dfcruzar = hc.sql(sent)
    dfcruzar.show()
    dfcruzar.printSchema()
    exit(0)


def ultimo_id_cargue(hc, sigla):
    
    sql = "(select idcargue from result_cargue('{0}', 'tabla')) as idcargue".format(sigla)
    df = cm.get_Query_Postgresql(hc, sql)
    return df
def preproc_producto_enc(hc, dicc, sigla):
    
    df = ultimo_id_cargue(hc, sigla)
    df = df.toPandas()
    idc = df['idcargue'][0]
    
    sent = "select * from stage_uiaf.te10_encabezado where idcargue = {0}".format(idc)
    dfenc = hc.sql(sent)
    dfenc = dfenc.toPandas()
    codent = dfenc['codigo_entidad'][0]
    return codent
    
    
    
    

