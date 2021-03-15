from pyspark import SparkContext, HiveContext, SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
import pandas as pd
import common as cm
import utils
from pyspark.sql.functions import *
import time
import tareas_2 as t2
import tareas_1 as t1
import preprocesamiento as pre
from datetime import datetime,date
import os
import gestioncargue
import shutil
import hdfs

Carpeta_local = os.path.dirname(os.path.abspath(__file__))+ "/temp_etl"

## Ejemplo Disparar ETL de Efectivo
"sudo spark-submit --jars /opt/cloudera/parcels/CDH/jars/postgresql-42.2.5.jar main.py -m TE10 -p 2020/11 -vvvv"

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

def carga_origen(hc, df, periodo, idproceso, idcargue_g):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Data con el get_proceso origen-destino de la base de datos de gestion.
    periodo : TYPE: string
        DESCRIPTION: Periodo del proceso a cargar.
    idproceso : TYPE: int
        DESCRIPTION: Id del proceso seleccionado con la sigla.
    idcargue_g : TYPE: int
        DESCRIPTION: Numero de cargue incremental de la base de datos de gestion.

    Returns
    -------
    total : TYPE: int
        DESCRIPTION: Numero de registros de la tabla principal a cargar.

    """
    cm.verbose_c('Aqui inicial el ETL de Efectivo paso 1',2)
    #cm.verbose_c(periodo,2)
    #cm.verbose_c(idproceso,2)
    #cm.verbose_c(idcargue_g,2)
    
    #df.show()
    ###################Generacion de diccionario de variables origen-destino########
    dicc, inc = gen_dicc(hc, df)
    #cm.verbose_c(dicc,2)
    
    if dicc['tipocatalogo_Or'] == 'Archivo':
        if periodo=='0':
            meses = ('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12')
            periodo=str(date.today().year) + "/" + meses[date.today().month]
        
        origen= dicc['basedatos_Or'] + dicc['nombrecatalogo_Or'] +"/"+ str(periodo)+"/Validos/"
        tempo=Carpeta_local +'/' + dicc['nombrecatalogo_Or'] +"/"+ str(periodo) +"/"
        HDFS_TEMP_PATH = cm.GLOBAL_CONFIG.get("hdfs_paht_landing")  +'/' +dicc['nombrecatalogo_Or'] +"/"+ str(periodo) +"/"
        #cm.verbose_c(origen,2)
        #cm.verbose_c(tempo,2)
        #cm.verbose_c(HDFS_TEMP_PATH,2)

        if  cm.GLOBAL_CONFIG.get("local"):
            x = 1
            while x <= inc: 
                dicc['basedatos_Ds_{}'.format(x)]=""
                x+=1
            HDFS_TEMP_PATH=tempo
        else:
            HDFS_TEMP_PATH = cm.GLOBAL_CONFIG.get("hdfs_paht_landing")  +'/' +dicc['nombrecatalogo_Or'] +"/"+ str(periodo) +"/"
        ###############El temporal de hdfs cambia cuando local esta activado###########
        #cm.verbose_c(HDFS_TEMP_PATH,2)
        cm.verbose_c(dicc,2)

        destino=origen.replace("Validos/", "Final/Procesados/")
        
        files = cm.files(origen)
        cm.verbose_c(destino,2)
        cm.verbose_c(files,2)
        
        
        if (len(files) == 0):
            observaciones = "No existen  archivos para procesar en la carpeta " + origen
            cm.verbose_c(observaciones, 1)
            gestioncargue.marcar_fin(hc, idcargue_g, 2, datetime.today(), 0, observaciones)
            exit(0)
        
        
        
        #cm.verbose_c(mySchema,2)
        cm.verbose_c(origen+files[0][0],2)
        
        total=0
        for file in files:
            cm.verbose_c(file,2)
            if file[1].lower()=='txt'  or  file[1].lower()=='csv' :
                #total= total + cargar_archivo(Hc,origen +file[0], idproceso,separador_Or,basedatos_Ds,catalogo_Ds,mySchema,HDFS_TEMP_PATH)
                total = total + cargar_archivo(hc, origen+file[0], idproceso, dicc, HDFS_TEMP_PATH)
            
            else:
                if os.path.exists(tempo+file[0]):
                    os.remove(tempo+file[0])
                try:

                    shutil.unpack_archive(origen+file[0],tempo)

                except IOError as err:
                    observaciones = "Error decomprimiendo el archivo, %s, %s " % (origen+file[0] , format(err))
                    cm.verbose_c(observaciones, 1)
                    gestioncargue.marcar_fin(hc, idcargue_g, 3, datetime.today(), 0, observaciones)
                    continue

                files_des = cm.files(tempo)
                for file_d in files_des:
                    total= total + cargar_archivo(hc, origen+file[0], idproceso, dicc, HDFS_TEMP_PATH)
                    os.remove(tempo + file_d[0])
            
            shutil.copy(origen + file[0], destino)
            

    return total


def cargar_archivo(hc, archivo, idproceso, dicc, HDFS_TEMP_PATH):
    """
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    archivo : TYPE: string
        DESCRIPTION: Directorio de la ubicacion del archivo a cargar.
    idproceso : TYPE: int
        DESCRIPTION: Id del proceso seleccionado con la sigla.
    dicc : TYPE: dict
        DESCRIPTION: Diccionario con las variables origen-destino de get_proceso.
    HDFS_TEMP_PATH : TYPE: string
        DESCRIPTION: Directorio con la ubicacion en el hdfs donde se cargara temporalmente.

    Returns
    -------
    regis : TYPE: int
        DESCRIPTION: Cantidad de registros a cargar de la tabla principal.

    """
    observacion='Procesando Archivo ' + archivo
    cm.verbose_c(observacion, 0)
    cm.verbose_c(idproceso, 2)
    fecha_cargue = datetime.now()
    idcargue = gestioncargue.marcar_inicio_cargue_archivo(hc, idproceso, fecha_cargue,observacion)
    enc=utils.detectar_codec(archivo)
    
    
    cm.verbose_c('Cargue con HDFS ' + HDFS_TEMP_PATH+archivo[archivo.rfind("/")+1 :], 2)
    if  cm.GLOBAL_CONFIG.get("local"):
         shutil.copy(archivo,HDFS_TEMP_PATH+archivo[archivo.rfind("/")+1 :])
    else:
        hdfs.copyFromLocal(archivo,HDFS_TEMP_PATH)
        
    cm.verbose_c(HDFS_TEMP_PATH,2)
    
    dataset, head, pie = leer_Archivo(hc, HDFS_TEMP_PATH+archivo[archivo.rfind("/")+1 :], dicc['separador_Or'], False, fecha_cargue, idcargue, enc, dicc)
    
    #############Carga de datasets de Efectivo en Hive#######################
    regis=cm.save_df_hive(hc,dataset,dicc['basedatos_Ds_2'], dicc['nombrecatalogo_Ds_2'],idcargue)
    cm.save_df_hive(hc,head,dicc['basedatos_Ds_1'], dicc['nombrecatalogo_Ds_1'], idcargue)
    cm.save_df_hive(hc,pie,dicc['basedatos_Ds_3'], dicc['nombrecatalogo_Ds_3'], idcargue)
    
    if  not cm.GLOBAL_CONFIG.get("local"):
        hdfs.deletefileHDFS(HDFS_TEMP_PATH+archivo[archivo.rfind("/")+1 :])


    observacion = "Archivo " + archivo + " cargado correctamente"
    cm.verbose_c(observacion, 0)
    gestioncargue.marcar_fin_cargue_archivo(hc, idcargue, 2, datetime.now(), regis,
                                            observacion)
    
    return regis

def leer_Archivo(Hc,archivo,separador,pandas,fecha_cargue,idcargue,enc, dicc):
    """
    Parameters
    ----------
    Hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    archivo : TYPE: string
        DESCRIPTION: Directorio de la ubicacion del archivo a cargar.
    separador : TYPE: string
        DESCRIPTION: Separador a utilizar en la carga.
    pandas : TYPE: Dataframe de pandas.
        DESCRIPTION: Dataframe de pandas False.
    fecha_cargue : TYPE: string
        DESCRIPTION: fecha en la cual se realiza la carga del proceso que se esta trabajando.
    idcargue : TYPE: int
        DESCRIPTION: Numero del cargue que se realiza registrado en la base de datos de gestion.
    enc : TYPE: string
        DESCRIPTION: Codificacion ASCII del dataset a cargar.
    dicc : TYPE: dict
        DESCRIPTION: Diccionario con las variables origen-destino de get_proceso.

    Returns
    -------
    dataset : TYPE: Dataframe de pyspark.
        DESCRIPTION: Dataframe detalles de proceso conformado por 3 destinos.
    head : TYPE: Dataframe de pyspark.
        DESCRIPTION: Dataframe encabezado.
    pie : TYPE: Dataframe de pyspark
        DESCRIPTION: Dataframe de pie de pagina.
    """
    if  separador=='F':

            dataset = Hc.read.load(archivo, format="csv", sep="|",
                              header=None,  charset=enc['encoding'])
            
            head, pie, dataset = stage_uiaf_efectivo_2(Hc, dataset, dicc)
            #head, pie, dataset = ajuste_efectivo(Hc, head, pie, dataset)
            
            """
            cols = []
            pos=1
            for i  in range(len(shema[1])):
                cols.append(trim(dataset._c0.substr(pos, shema[1][i])).cast(str(shema[0][i].dataType)[0:-4]).alias(shema[0][i].name))
                pos+=shema[1][i]
            dataset=dataset.select(cols)
    
    else:

            dataset = Hc.read.load(archivo, format="csv", sep=separador,
                              header=None, schema=shema[0], charset=enc['encoding'])
    """
    if utils.find(dataset.columns,"fecha_cargue"):
            dataset = dataset.select(dataset.columns[:-2], ).withColumn('fecha_cargue', lit(datetime.now())).withColumn('idcargue',
                                                                                                             lit(idcargue))
    else:
            dataset=dataset.withColumn('fecha_cargue', lit(datetime.now())).withColumn('idcargue',lit(idcargue))
    if utils.find(head.columns,"fecha_cargue"):
            head = head.select(head.columns[:-2], ).withColumn('fecha_cargue', lit(datetime.now())).withColumn('idcargue',
                                                                                                             lit(idcargue))
    else:
            head = head.withColumn('fecha_cargue', lit(datetime.now())).withColumn('idcargue',lit(idcargue))
    if utils.find(pie.columns,"fecha_cargue"):
            pie = pie.select(pie.columns[:-2], ).withColumn('fecha_cargue', lit(datetime.now())).withColumn('idcargue',
                                                                                                             lit(idcargue))
    else:
            pie=pie.withColumn('fecha_cargue', lit(datetime.now())).withColumn('idcargue',lit(idcargue))
    
    """
    #No tiene consecutivo lo agregamos
    if not utils.find(dataset.columns,"consecutivo"):
        col = dataset.columns
        col.insert(0, row_number().over(Window.partitionBy('idcargue').orderBy('idcargue')).alias("consecutivo"))
        dataset=dataset.select(col)
    """
    return dataset, head, pie


def sentencia_1(shema):
    """
    Parameters
    ----------
    shema : TYPE: Schema de pyspark.
        DESCRIPTION: Schema del dataframe a cargar.

    Returns
    -------
    query : TYPE: string
        DESCRIPTION: Consulta en sql autocreada a partir del schema proporcionado.

    """
    cad1 = 'cast(trim(substring(_c0, {0}, {1})) as {3}) as {2}'
    sent = "select {0} from dataset"
    cad = ''
    ini = 1
    for index,i in enumerate(shema[1]):
        #cm.verbose_c(shema[0][index], 2)
        trin = cad1.format(ini, int(shema[1][index]), shema[0][index].name, tipos_hiveql(str(shema[0][index].dataType)))
        cad += trin
        cad += ', '
        ini += int(shema[1][index])
    if cad[-2:] == ', ':
        cad = cad[:-2]
    query = sent.format(cad)
    query = normalizar(query)
    return query

def normalizar(cadena):
    """
    Parameters
    ----------
    cadena : TYPE: string
        DESCRIPTION: Cadena a normalizar.

    Returns
    -------
    cadena : TYPE: string
        DESCRIPTION: Cadena normalizada sin tildes.

    """
    cadena = cadena.replace('á', 'a') \
            .replace('é', 'e') \
            .replace('í', 'i') \
            .replace('ó', 'o') \
            .replace('ú', 'u')
    return cadena

def tipos_hiveql(tipo):
    """
    Parameters
    ----------
    tipo : TYPE: string
        DESCRIPTION: Tipo a aplicar a sentencia sql.

    Returns
    -------
    TYPE: string
        DESCRIPTION: Tipo transformado a parametros de sentencia sql.

    """
    switcher = {
        'IntegerType':'int',
        'StringType':'string',
        'FloatType':'float',
        'TimestampType':'datetime',
        'LongType':'bigint'
    }
    return str(switcher[tipo])
    
def stage_uiaf_efectivo_2(Hc, dataset, dicc):
    """
    Parameters
    ----------
    Hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dataset : TYPE: Dataframe de pyspark.
        DESCRIPTION: Dataframe a procesar.
    dicc : TYPE: dict
        DESCRIPTION: Diccionario con las variables del proceso_get.

    Returns
    -------
    sdfencabezado : TYPE: Dataframe de pyspark
        DESCRIPTION: Dataframe encabezado.
    sdfpie : TYPE: Dataframe de pyspark
        DESCRIPTION: Dataframe de pie de pagina.
    dataset : TYPE: Dataframe de pyspark
        DESCRIPTION: Dataframe detalles.

    """
    ################Esquemas de cada dataset##########################
    shemaenc = cm.get_schema(Hc, dicc['idcatalogo_Ds_1'])
    shemapie = cm.get_schema(Hc, dicc['idcatalogo_Ds_3'])
    shema = cm.get_schema(Hc, dicc['idcatalogo_Ds_2'])
    ###############Dataset encabezado#######################
    dataset.registerTempTable('dataset')
    query = sentencia_1(shemaenc)
    #cm.verbose_c(query, 2)
    dataset = Hc.sql(query)
    #dataset.show()
    #exit(0)
    dataset.registerTempTable('dataset2')
    sdfencabezado = Hc.sql("select * from dataset2 where consecutivo = '0' and \
                     cantidad_registros is not null")
    #dfp = dataset.toPandas().astype(int, errors='ignore')
    #encabezado = dfp.head(1)
    #sdfencabezado = Hc.createDataFrame(encabezado, schema_1)
    
    ##################Dataset Pie#########################
    query = sentencia_1(shemapie)
    #cm.verbose_c(query, 2)
    dataset = Hc.sql(query)
    dataset.registerTempTable('dataset2')
    sdfpie = Hc.sql("select * from dataset2 where Consecutivo = '0' and \
                     cantidad_registros is not null")
    #dfp = dataset.toPandas()
    #pie = dfp.tail(1)
    #sdfpie = Hc.createDataFrame(pie, schema_2)
    
    ################Dataset total########################
    query = sentencia_1(shema)
    #cm.verbose_c(query, 2)
    dataset = Hc.sql(query)
    dataset.registerTempTable('dataset2')
    dataset = Hc.sql("select * from dataset2 where Consecutivo != '0'")
    
    return sdfencabezado, sdfpie, dataset







    





    
    