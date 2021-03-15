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
import preproc_dimdane as ddane
import preproc_dimmoneda as dmoneda
import preproc_dimtipoidentificacion as dtipoidentificacion
import preproc_dimtipoproducto as dtipoproducto
import preproc_dimpersona as dpersona
import preproc_dimproducto as dproducto
import preproc_dimactividad as dactividadeconomica
import preproc_dimpaises as dpais
import preproc_factefectivo as fefectivo

import inner_dimdane as idane
import inner_dimmoneda as imoneda
import inner_dimtipoidentificacion as itipoidentificacion
import inner_dimtipoproducto as itipoproducto
import inner_dimpersona as ipersona
import inner_dimproducto as iproducto
import inner_dimactividad as iactividadeconomica
import inner_dimpaises as ipais
import inner_factefectivo as ifefectivo

import right_dimdane as rdane
import right_dimmoneda as rmoneda
import right_dimtipoidentificacion as rtipoidentificacion
import right_dimtipoproducto as rtipoproducto
import right_dimpersona as rpersona
import right_dimproducto as rproducto
import right_dimactividad as ractividadeconomica
import right_dimpaises as rpais
import right_dimpersona as rpersona

actual = time.strftime("20%y-%m-%d %H:%M:%S")
actualsin = time.strftime("20%y-%m-%d")
actualsin
#######################Contextos de spark y hive################
sc = SparkContext()
hc = HiveContext(sc)

########################Procesamiento persona################################

#t2.incremental(hc, 'dwh_uiaf', 'dimpersonasistema', 'idpersonasistema')


def gen_dicc(hc, df):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Data con el get_proceso origen-destino de la base de datos de gestion.

    Returns
    -------
    dicc : TYPE: dict
        DESCRIPTION: Diccionario con las variables del proceso_get.
    inc : TYPE: int
        DESCRIPTION: Incremental del numero de destino que nos devuelve get_proceso.

    """
    dicc = {}
    dfp = df.toPandas()
    inc = 1
    for index,i in enumerate(dfp['tipocatalogo']):
        if dfp['origen'][index] == 'O':
            for n in dfp:
                dicc['{0}_Or'.format(str(n))] = str(dfp[n][index])
        elif dfp['origen'][index] == 'D':
            for n in dfp:
                dicc['{0}_Ds_{1}'.format(str(n), inc)] = str(dfp[n][index])
            inc+=1
    return dicc, inc

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
    
    if df.count() == 8 or df.count() == 7:
        reg = carga_origen(Hc, df, idproceso, idcargue_g, sigla)
    
    
    
    gestioncargue.marcar_fin_cargue_archivo(Hc, idcargue_g, 2, datetime.now(), reg,
                                            "Archivos cargados Correctamente")

            
def carga_origen(Hc, df, idproceso, idcargue_g, sigla):
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
    sigla : TYPE: String
        DESCRIPTION: Palabra clave acerca de el origen de las tablas a cargar.

    Returns
    -------
    total : TYPE: int
        DESCRIPTION: Numero de registros de la tabla principal a cargar.

    """
    cm.verbose_c('Aqui inicia el ETL de Efectivo paso 2',2)
    
    dicc, inc = gen_dicc(hc, df)
    
    if dicc['tipocatalogo_Or'] == 'Tabla':
        origen = dicc['nombrecatalogo_Or']
        bdorigen = dicc['basedatos_Or']
        tables = cm.table(Hc, origen, bdorigen)
        
        if tables == False:
            observaciones = "No existe la tabla " + origen
            cm.verbose_c(observaciones, 1)
            gestioncargue.marcar_fin(hc, idcargue_g, 2, datetime.today(), 0, observaciones)
            
    
        cm.verbose_c(origen, 2)
        
        total = cargar_tabla(Hc, origen, idproceso, dicc, df, sigla)
        
    return total
    
def cargar_tabla(Hc, origen, idproceso, dicc, df, sigla):
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
    sigla : TYPE: String
        DESCRIPTION: Palabra clave acerca de el origen de las tablas a cargar.

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
    diccfinal = procesamiento_inner(Hc, dicc, fecha_cargue, idcargue, df, sigla)
    
    
    #####Carga de datasets de Dim a Hive############################
    """
    "dfp['basedatos'][index]"
    cargasinner = 0
    for index,i in enumerate(dfp['nombrecatalogo']):
        try:
            if dfp['origen'][index] == 'O' or diccfinal[i] == 0:
                continue
        except KeyError:
            continue
        else:
            diccfinal[i].write.format("Hive").saveAsTable("%s.%s" % ('dwh_uiaf2', \
                        i), mode="append")
            cargasinner += 1
    """
    ##################Datasets de DIM cruze Right#########################
    #diccfinal = procesamiento_right_sistema(Hc, dicc, fecha_cargue, idcargue, df, sigla)
    
    #####Carga de datasets de Dim a Hive############################
    """
    cargasright = 0
    for index,i in enumerate(dfp['nombrecatalogo']):
        try:
            if dfp['origen'][index] == 'O' or diccfinal[i] == 0:
            continue
        except KeyError:
            continue
        else:
            diccfinal[i].write.format("Hive").saveAsTable("%s.%s" % (dfp['basedatos'][index], \
                        i), mode="append")
            cargasright += 1
    """
    
    observacion = "Tablas " + origen + " cargado correctamente"
    cm.verbose_c(observacion, 0)
    cm.verbose_c(cargasinner, 2)
    cm.verbose_c(cargasright, 2)
    gestioncargue.marcar_fin_cargue_archivo(hc, idcargue, 2, datetime.now(), '0',
                                            observacion)
    return '0'

def procesamiento_inner(Hc, diccget, fecha_cargue, idcargue, df, sigla):
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
    sigla : TYPE: String
        DESCRIPTION: Palabra clave acerca de el origen de las tablas a cargar.

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
            
            
                
        
            if i == 'dimdanedetallado':
                #########################Procesamiento dane############################
                dicc['campos'] = ['iddane']
                
                cm.verbose_c('Entro dane', 2)
                
                dfcruzar = ddane.clasificador(hc, dicc, sigla)


                diccfinal[i] = idane.clasificador(hc, dfcruzar, dicc, sigla)
                diccfinal[i].show()
                diccfinal[i].printSchema()
                exit(0)
                
                
                
            elif i == 'dimmonedadetallad':
                ########################Procesamiento moneda###########################
                cm.verbose_c('Entro moneda', 2)
                dicc['campos'] = ['codigo']
                
                dfcruzar = dmoneda.clasificador(hc, dicc, sigla)

                diccfinal[i] = imoneda.clasificador(hc, dfcruzar, dicc, sigla, actual)
                diccfinal[i].show()
                diccfinal[i].printSchema()
                exit(0)
                

            elif i == 'dimtipoidentificaciondetallad':
                ########################Procesamiento tipo identificacion detallado####################
                cm.verbose_c('Entro tipo identificacion', 2)
                dicc['campos'] = ['idtipoidentificacion']
                
                dfcruzar = dtipoidentificacion.clasificador(hc, dicc, sigla)
                
                
                diccfinal[i] = itipoidentificacion.clasificador(hc, dfcruzar, dicc, sigla, actual, actualsin)
                diccfinal[i].show()
                diccfinal[i].printSchema()
                exit(0)

                #t1.cruze_actividad_sistema(hc, dfcruzar, dicc)

            elif i == 'dimactividadeconomicadetallad':
                cm.verbose_c('Entro actividad economica', 2)
                dicc['campos'] = ['idactividadeconomica']

                dfcruzar = dactividadeconomica.clasificador(hc, dicc, sigla)

                diccfinal[i] = iactividadeconomica.clasificador(hc, dfcruzar, dicc, sigla, actual)
                diccfinal[i].show()
                diccfinal[i].printSchema()
                exit(0)
            elif i == 'dimpaisesdetallad':
                cm.verbose_c('Entro paises', 2)
                dicc['campos'] = ['codigopais']

                dfcruzar = dpais.clasificador(hc, dicc, sigla)

                diccfinal[i] = ipais.clasificador(hc, dfcruzar, dicc, sigla, actual)
                diccfinal[i].show()
                diccfinal[i].printSchema()
                exit(0)


            elif i == 'dimtipoproductodetallad':
                ########################Procesamiento tipo producto detallado####################
                cm.verbose_c('Entro tipo producto', 2)
                dicc['campos'] = ['idtipoproducto']
                dicc['prepo'] = ['tipo_producto']
                dfcruzar = dtipoproducto.clasificador(hc, dicc, sigla)
                
                
                diccfinal[i] = itipoproducto.clasificador(hc, dfcruzar, dicc, sigla, actual, actualsin)
                diccfinal[i].show()
                diccfinal[i].printSchema()
                exit(0)
                #t1.cruze_actividad_sistema(hc, dfcruzar, dicc)
                
            
            elif i == 'dimpersonadetallad':
                """
                Ayuda: campos = Lista de campos para realizar cruze con tabla de hive
                prepo = diccionario de campos para realizar el debido preprocesamiento al dataset
                """
                
                dicc['campos'] = ['identificacion','idtipoidentificacion']
                
                cm.verbose_c('Entro persona', 2)
                
                dfcruzar = dpersona.clasificador(hc, dicc, sigla)




                diccfinal[i] = ipersona.clasificador(hc, dfcruzar, dicc, sigla, actual, actualsin, idcargue)
                
                diccfinal[i].show()
                exit(0)
                
                #t1.cruze_persona_sistema(hc, dfcruzar, dicc, actualsin)


            elif i == 'dimproductodetallad':
                dicc['campos'] = ['numero']
                dfcruzar = dproducto.clasificador(hc, dicc, sigla)
                dfcruzar.show()
                dfcruzar.printSchema()
                exit(0)
                
                diccfinal[i] = iproducto.clasificador(hc, dfcruzar, dicc, sigla, actual, actualsin)
                diccfinal[i].printSchema()
                exit(0)



            elif i == 'factefectiv':
                ########################Procesamiento paises###########################
                cm.verbose_c('Entro factefectivo', 2)

                dicc['campos'] = ['']
                dfcruzar = fefectivo.clasificador(hc, dicc, sigla)
                
                
                
                
                #diccfinal[i] = ifefectivo.clasificador(hc, dfcruzar, dicc, sigla)
                #diccfinal[i].show()
                #diccfinal[i].printSchema()
                #exit(0)
    
    return diccfinal

def procesamiento_right_sistema(Hc, diccget, fecha_cargue, idcargue, df, sigla):
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
    sigla : TYPE: String
        DESCRIPTION: Palabra clave acerca de el origen de las tablas a cargar.

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
            
            if i == "dimdanedetallad":
                ##########################Procesamiento tipoidentificacion####################
                
                dicc['campos'] = ['iddane']
                cm.verbose_c('Entro dane', 2)

                dfcruzar = ddane.clasificador(hc, dicc, sigla)


                diccfinal[i] = rdane.clasificador(hc, dfcruzar, dicc, sigla)
                diccfinal[i].show()
                diccfinal[i].printSchema()
                exit(0)

            elif i == 'dimmonedadetallad':
                #########################Procesamiento dane############################
                dicc['campos'] = ['codigo']
                
                cm.verbose_c('Entro moneda', 2)
                
                dfcruzar = dmoneda.clasificador(hc, dicc, sigla)
                

                diccfinal[i] = rmoneda.clasificador(hc, dfcruzar, dicc, sigla)
                diccfinal[i].show()
                diccfinal[i].printSchema()
                exit(0)



            elif i == 'dimtipoidentificaciondetallad':
                ########################Procesamiento tipo identificacion detallado####################
                cm.verbose_c('Entro tipo identificacion', 2)
                dicc['campos'] = ['tipoidentificacion']
                
                dfcruzar = dtipoidentificacion.clasificador(hc, dicc, sigla)

                
                diccfinal[i] = rtipoidentificacion.clasificador(hc, dfcruzar, dicc, sigla)
                diccfinal[i].show()
                diccfinal[i].printSchema()
                exit(0)

            elif i == 'dimactividadeconomicadetallad':
                cm.verbose_c('Entro actividad economica', 2)
                dicc['campos'] = ['idactividadeconomica']

                dfcruzar = dactividadeconomica.clasificador(hc, dicc, sigla)

                diccfinal[i] = ractividadeconomica.clasificador(hc, dfcruzar, dicc, sigla, actual)
                diccfinal[i].show()
                diccfinal[i].printSchema()
                exit(0)

            elif i == 'dimpaisesdetallad':
                cm.verbose_c('Entro paises', 2)
                dicc['campos'] = ['codigopais']

                dfcruzar = dpais.clasificador(hc, dicc, sigla)

                diccfinal[i] = rpais.clasificador(hc, dfcruzar, dicc, sigla, actual)
                diccfinal[i].show()
                diccfinal[i].printSchema()
                exit(0)


            elif i == 'dimtipoproductodetallad':
            ########################Procesamiento paises###########################
                cm.verbose_c('Entro tipoproductosistema', 2)
                dicc['campos'] = ['idtipoproducto']
                dicc['prepo'] = ['tipo_producto']
                dfcruzar = dtipoproducto.clasificador(hc, dicc, sigla)
                
                diccfinal[i] = rtipoproducto.clasificador(hc, dfcruzar, dicc, actualsin, sigla)
                
            

            elif i == 'dimpersonadetallad':
                ########################Procesamiento actividad economica####################
                dicc['campos'] = ['identificacion','idtipoidentificacion']
                
                cm.verbose_c('Entro persona', 2)
                
                dfcruzar = dpersona.clasificador(hc, dicc, sigla)

                diccfinal[i] = rpersona.clasificador(hc, dfcruzar, dicc, sigla, actual, actualsin, idcargue)
                diccfinal[i].show()
                diccfinal[i].printSchema()
                exit(0)


            elif i == 'dimproductodetallad':
                ########################Procesamiento moneda###########################
                cm.verbose_c('Entro producto', 2)
                dicc['campos'] = ['numero']
                dfcruzar = dproducto.clasificador(hc, dicc, sigla)

                
                diccfinal[i] = rproducto.clasificador(hc, dfcruzar, dicc, sigla, actual, actualsin)
                

            elif i == 'factefectiv':
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
    






Ingesta_paso_2(hc, 'TE10')

















