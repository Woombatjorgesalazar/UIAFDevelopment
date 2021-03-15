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



def preproc_tipo_identificacion(hc, dicc):
    ##########################Condicion#############################
    if 'prepo' not in dicc:
        return 'Los campos de preprocesamiento no se encuentran en el diccionario base'
    if len(dicc['prepo']) != 1:
        return 'La longitud del diccionario base debe ser igual a un elemento'
    campo = dicc['prepo'][0]
    ####################################Extraer dfcruzar###########################
    sent = "select * from stage_uiaf.adres_bduda"
    dfcruzar = hc.sql(sent).groupBy(campo).count()
    ##############################Adaptar cruze con columnas iguales###############################
    dfcruzar = dfcruzar.select(col(campo).alias("tipoidentificacion"))
    #dfcruzar.registerTempTable("dfcruzar")
    
    return dfcruzar


def preproc_dane(hc, dicc):
    ######################Condicion##################################
    if 'prepo' not in dicc:
        return 'Los campos de preprocesamiento no se encuentran en el diccionario base'
    
    if len(dicc['prepo']) == 2:
        """
        Ayuda el codigo de departamento y de municipio que se escriban 
        correspondientemente en la lista de python.
        """
        
        #############Seleccion y ajuste de acuerdo a la tabla de stage_uiaf################
        iddepartamento = dicc['prepo'][0]
        idmunicipio = dicc['prepo'][1]
        sent = "select * from stage_uiaf.adres_bduda"
        dfcruzar = hc.sql(sent)
        dfcruzar.registerTempTable("prueba")
        dfcruzar = hc.sql("select concat(if(length({0}) = 1, concat('0',{0}), {0}), \
                          right(concat('000', {1}), 3)) as iddane from prueba" \
                            .format(iddepartamento,idmunicipio))
        dfcruzar = dfcruzar.groupBy('iddane').count()
        
    if len(dicc['prepo']) == 1:
        return 'Solo existe un campo para preprocesar'
    
    return dfcruzar


def preproc_persona(hc, dicc):
    ############################Condicion################################
    
    sent = "select * from stage_uiaf.adres_bduda"
    dfcruzar = hc.sql(sent)
    dfcruzar.registerTempTable("prueba")
    dfcruzar = hc.sql("select *, concat(if(length(nivelsisben) = 1, \
                      concat('0',nivelsisben), nivelsisben), \
        right(concat('000', coddepafi), 3)) as iddanevive, \
        concat(priapeafi,' ', segapeafi, ' ', prinomafi) as nombresrazonsocial \
        from prueba")
    
    #dfcruzar.show()
    
    dfcruzar.registerTempTable("prueba")
    
    dfcruzar = hc.sql("""select from_unixtime(unix_timestamp(t1.fecnacafi, 'dd/MM/yyyy'), 'yyyy-MM-dd') as fechanacimientocreacion, 
                      t1.tipdocafi as identificacion, 
                      t2.idtipoidentificacion, t1.iddanevive, t1.nombresrazonsocial, 
                      from_unixtime(unix_timestamp(t1.tipafi, 'dd/MM/yyyy'), 'yyyy-MM-dd') as fechatransaccion
                      from prueba t1 inner join dwh_uiaf.dimtipoidentificacionsistema t2 
                      on trim(t1.segnomafi) = trim(t2.tipoidentificacion)""")
    
    dfcruzar.registerTempTable("prueba")
    dfcruzar = hc.sql("select t1.*,t2.idfecha as idfechatransaccion from prueba t1 \
                      inner join dwh_uiaf.dimfecha t2 \
                      on trim(t1.fechatransaccion) = trim(t2.fecha)")
    dfcruzar = dfcruzar.drop('fechatransaccion')
    
    
    return dfcruzar





