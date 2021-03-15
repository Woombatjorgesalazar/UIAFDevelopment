from pyspark import SparkContext, HiveContext, SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
from datetime import datetime,date
import common as cm
import utils
from pyspark.sql.functions import *
import time
import tareas_2 as t2
import tareas_1 as t1
import preprocesamiento as pre
import gestioncargue
import etl_Efectivo as ef

actual = time.strftime("20%y-%m-%d %H:%M:%S")
actualsin = time.strftime("20%y-%m-%d")
actualsin
#######################Contextos de spark y hive################
sc = SparkContext()
hc = HiveContext(sc)

########################Procesamiento persona################################

#t2.incremental(hc, 'dwh_uiaf', 'dimpersonasistema', 'idpersonasistema')


def Ingesta_paso_2(Hc, sigla):
    """
    Parameters
    ----------
    Hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de Hive
    sigla : TYPE: string
        DESCRIPTION: Sigla del proceso a trabajar

    Returns
    -------
    None.

    """
    cm.GLOBAL_CONFIG["verbosity"] = 3
    fecha_inicio = datetime.now()
    sql ="(select * from get_procepaso_2('%s')) as proceso" % (sigla)
    
    cm.verbose_c(sql, 3)
    df = cm.get_Query_Postgresql(Hc, sql)
    
    # MARCAMOS EL INICIO general DE LA EJECUCION
    idproceso=df.select(df["idproceso"]).where("origen=='O'").collect()[0]['idproceso']
    #cm.verbose_c(idproceso, 2)
    idcargue_g=gestioncargue.marcar_inicio(Hc, idproceso,fecha_inicio)
    
    cm.verbose_c(idcargue_g, 2)
    cm.verbose_c(df.count(), 2)
    
    if df.count() == 6:
        reg = carga_origen(Hc, df, idproceso, idcargue_g)
    
    
    gestioncargue.marcar_fin_cargue_archivo(Hc, idcargue_g, 2, datetime.now(), reg,
                                            "Archivos cargados Correctamente")

            
def carga_origen(Hc, df, idproceso, idcargue_g):
    """
    Parameters
    ----------
    Hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Dataframe del proceso a realizar.
    idproceso : TYPE: int 
        DESCRIPTION: Id del proceso seleccionado con la sigla.
    idcargue_g : TYPE: int
        DESCRIPTION. Numero de cargue incremental de la base de datos de gestion.

    Returns
    -------
    total : TYPE: int
        DESCRIPTION: Numero de registros de la tabla principal a cargar.

    """
    cm.verbose_c('Aqui inicial el ETL de ADRES paso 2',2)
    
    dicc, inc = ef.gen_dicc(hc, df)
    
    if dicc['tipocatalogo_Or'] == 'Tabla':
        origen = dicc['nombrecatalogo_Or']
        bdorigen = dicc['basedatos_Or']
        tables = cm.table(Hc, origen, bdorigen)
        
        if tables == False:
            observaciones = "No existe la tabla " + origen
            cm.verbose_c(observaciones, 1)
            gestioncargue.marcar_fin(hc, idcargue_g, 2, datetime.today(), 0, observaciones)
            
    
        cm.verbose_c(origen, 2)
        
        total = cargar_tabla(Hc, origen, idproceso, dicc, df)
        
    return total
    
def cargar_tabla(Hc, origen, idproceso, dicc, df):
    """
    Parameters
    ----------
    Hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de Hive.
    origen : TYPE: string
        DESCRIPTION: nombre del catalogo origen
    idproceso : TYPE: int
        DESCRIPTION: Id del proceso seleccionado con la sigla.
    dicc : TYPE: dict
        DESCRIPTION: Diccionario con las variables origen-destino de get_proceso.
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Dataframe del proceso a realizar.

    Returns
    -------
    '0' : TYPE: string
        DESCRIPTION: Cantidad de registros cargados en tabla principal.

    """
    observacion='Procesando tabla ' + origen
    cm.verbose_c(observacion, 0)
    cm.verbose_c(idproceso, 2)
    fecha_cargue = datetime.now()
    idcargue = gestioncargue.marcar_inicio_cargue_archivo(hc, idproceso, fecha_cargue,observacion)
    dfp = df.toPandas()
    ##################Datasets de DIM cruze Inner###########################
    diccfinal = procesamiento_inner(Hc, dicc, fecha_cargue, idcargue, df)
    
    
    #####Carga de datasets de Dim a Hive############################
    cargasinner = 0
    for index,i in enumerate(dfp['nombrecatalogo']):
        if dfp['origen'][index] == 'O' or diccfinal[i] == 0:
            continue
        else:
            diccfinal[i].write.format("Hive").saveAsTable("%s.%s" % (dfp['basedatos'][index], \
                        i), mode="append")
            cargasinner += 1
    
    ###################Datasets de DIM cruze Right#########################
    diccfinal = procesamiento_right_sistema(Hc, dicc, fecha_cargue, idcargue, df)
    
    #####Carga de datasets de Dim a Hive############################
    cargasright = 0
    for index,i in enumerate(dfp['nombrecatalogo']):
        if dfp['origen'][index] == 'O' or diccfinal[i] == 0:
            continue
        else:
            diccfinal[i].write.format("Hive").saveAsTable("%s.%s" % (dfp['basedatos'][index], \
                        i), mode="append")
            cargasright += 1
    
    observacion = "Tablas " + origen + " cargado correctamente"
    cm.verbose_c(observacion, 0)
    cm.verbose_c(cargasinner, 2)
    cm.verbose_c(cargasright, 2)
    gestioncargue.marcar_fin_cargue_archivo(hc, idcargue, 2, datetime.now(), '0',
                                            observacion)
    return '0'

def procesamiento_right_sistema(Hc, diccget, fecha_cargue, idcargue, df):
    """
    Parameters
    ----------
    Hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de Hive
    diccget : TYPE: dict
        DESCRIPTION: Diccionario con las variables del proceso_get.
    fecha_cargue : TYPE: string
        DESCRIPTION: fecha en la cual se realiza la carga del proceso que se esta trabajando.
    idcargue : TYPE: int
        DESCRIPTION: Numero del cargue que se realiza registrado en la base de datos de gestion.
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Data con el get_proceso origen-destino de la base de datos de gestion.

    Returns
    -------
    diccfinal : TYPE: dict
        DESCRIPTION: Diccionario con los dataframes de pyspark dimsistema en el cruze right.

    """
    dfp = df.toPandas()
    cont = 0
    diccfinal = {}
    for index,i in enumerate(dfp['nombrecatalogo']):
        if index == 0:
            continue
        else:
            dicc = {}
            dicc['idsistema'] = df.select(df["idsistema"]).where("origen=='O'").collect()[0]['idsistema']
            dicc['idcatalogo'] = df.select(df['idcatalogo']).where("origen=='O'").collect()[0]['idcatalogo']
            #cm.verbose_c(dicc['idsistema'], 2)
            #cm.verbose_c(dicc['idcatalogo'], 2)
            
            if i == "dimtipoidentificacionsistema":
                ##########################Procesamiento tipoidentificacion####################
                dicc['prepo'] = ['segnomafi']
                cm.verbose_c('Entro tipoidentificacion', 2)
                cont+=1
                dicc['campos'] = ['tipoidentificacionsistema']
                dfcruzar = pre.preproc_tipo_identificacion(hc, dicc)
                
                diccfinal[i] = t1.cruze_tipo_identificacion_sistema(hc, dfcruzar, dicc)
                
            elif i == 'dimdanesistema':
                #########################Procesamiento dane############################
                dicc['campos'] = ['codigodanesistema']
                dicc['prepo'] = ['nivelsisben','coddepafi']
                cm.verbose_c('Entro dane', 2)
                cont+=1
                dfcruzar = pre.preproc_dane(hc, dicc)
                
                diccfinal[i] = t1.cruze_dane_sistema(hc, dfcruzar, dicc)
                
            elif i == 'dimpaisessistema':
                ########################Procesamiento paises###########################
                cm.verbose_c('Entro pais', 2)
                cont+=1
                #diccfinal[i] = t1.cruze_paises_sistema(hc,dfcruzar, dicc, actual)
                diccfinal[i] = 0
            elif i == 'dimmonedasistema':
                ########################Procesamiento moneda###########################
                cm.verbose_c('Entro moneda', 2)
                cont+=1
                
                #t1.cruze_moneda_sistema(hc,dfcruzar, dicc)
                diccfinal[i] = 0
            elif i == 'dimactividadeconomicasistema':
                ########################Procesamiento actividad economica####################
                cm.verbose_c('Entro actividad', 2)
                cont+=1
                
                #t1.cruze_actividad_sistema(hc, dfcruzar, dicc)
                diccfinal[i] = 0
            elif i == 'dimpersonasistema':
                """
                Ayuda: campos = Lista de campos para realizar cruze con tabla de hive
                prepo = diccionario de campos para realizar el debido preprocesamiento al dataset
                """
                cont+=1
                dicc['campos'] = ['identificacion','iddanevive']
                dicc['prepo'] = {'iddepartamento':'nivelsisben',
                           'idmunicipio':'coddepafi',
                           'fechanacimientocreacion':'fecnacafi',
                           'identificacion':'tipdocafi',
                           'tipoidentificacion':'segnomafi'
                          }
                cm.verbose_c('Entro persona', 2)
                dfcruzar = pre.preproc_persona(hc, dicc)
                
                diccfinal[i] = t1.cruze_persona_sistema(hc, dfcruzar, dicc, actualsin)
                
    
    return diccfinal
    
def procesamiento_inner(Hc, diccget, fecha_cargue, idcargue, df):
    """
    Parameters
    ----------
    Hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    diccget : TYPE: dict
        DESCRIPTION: Diccionario con las variables del proceso_get.
    fecha_cargue : TYPE: string
        DESCRIPTION: fecha en la cual se realiza la carga del proceso que se esta trabajando.
    idcargue : TYPE: int
        DESCRIPTION: Numero del cargue que se realiza registrado en la base de datos de gestion.
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Data con el get_proceso origen-destino de la base de datos de gestion.

    Returns
    -------
    diccfinal : TYPE: dict
        DESCRIPTION: Diccionario con los dataframes de pyspark dimsistema en el cruze inner.

    """
    dfp = df.toPandas()
    diccfinal = {}
    for index,i in enumerate(dfp['nombrecatalogo']):
        if index == 0:
            continue
        else:
            dicc = {}
            dicc['idsistema'] = df.select(df["idsistema"]).where("origen=='O'").collect()[0]['idsistema']
            dicc['idcatalogo'] = df.select(df['idcatalogo']).where("origen=='O'").collect()[0]['idcatalogo']
            #cm.verbose_c(dicc['idsistema'], 2)
            #cm.verbose_c(dicc['idcatalogo'], 2)
            
            if i == "dimtipoidentificacionsistema":
                ##########################Procesamiento tipoidentificacion####################
                dicc['campos'] = ['tipoidentificacion']
                dicc['prepo'] = ['segnomafi']
                cm.verbose_c('Entro tipoidentificacion', 2)
                
                dfcruzar = pre.preproc_tipo_identificacion(hc, dicc)
                diccfinal[i] = t2.cruze_tipo_identificacion_sistema(hc, dfcruzar, dicc)
                
        
            elif i == 'dimdanesistema':
                #########################Procesamiento dane############################
                dicc['campos'] = ['iddane']
                dicc['prepo'] = ['nivelsisben','coddepafi']
                cm.verbose_c('Entro dane', 2)
        
                dfcruzar = pre.preproc_dane(hc, dicc)
                diccfinal[i] = t2.cruze_dane_sistema(hc, dfcruzar, dicc)
                #t1.cruze_dane_sistema(hc, dfcruzar, dicc)
            elif i == 'dimpaisessistema':
                ########################Procesamiento paises###########################
                cm.verbose_c('Entro pais', 2)
                
                diccfinal[i] = t2.cruze_paises_sistema(hc, dicc, actual)
                #t1.cruze_paises_sistema(hc,dfcruzar, dicc, actual)
            elif i == 'dimmonedasistema':
                ########################Procesamiento moneda###########################
                cm.verbose_c('Entro moneda', 2)
                
                diccfinal[i] = t2.cruze_moneda_sistema(hc, dicc, actual)
                #t1.cruze_moneda_sistema(hc,dfcruzar, dicc)
            elif i == 'dimactividadeconomicasistema':
                ########################Procesamiento actividad economica####################
                cm.verbose_c('Entro actividad', 2)
                
                diccfinal[i] = t2.cruze_actividad_sistema(hc, dicc, actual)
                #t1.cruze_actividad_sistema(hc, dfcruzar, dicc)
            elif i == 'dimpersonasistema':
                """
                Ayuda: campos = Lista de campos para realizar cruze con tabla de hive
                prepo = diccionario de campos para realizar el debido preprocesamiento al dataset
                """
                
                dicc['campos'] = ['identificacion','idtipoidentificacion','fechanacimientocreacion']
                dicc['prepo'] = {'iddepartamento':'nivelsisben',
                           'idmunicipio':'coddepafi',
                           'fechanacimientocreacion':'fecnacafi',
                           'identificacion':'tipdocafi',
                           'tipoidentificacion':'segnomafi'
                          }
                cm.verbose_c('Entro persona', 2)
                dfcruzar = pre.preproc_persona(hc, dicc)
                #dfcruzar.show()
                #dfcruzar.printSchema()
                
                diccfinal[i] = t2.cruze_persona_sistema(hc, dfcruzar, dicc, actual, actualsin)
                #t1.cruze_persona_sistema(hc, dfcruzar, dicc, actualsin)
    
    
    return diccfinal





Ingesta_paso_2(hc, 'BDUDA')