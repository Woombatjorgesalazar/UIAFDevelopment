import os,shutil,glob
from datetime import datetime,date
#import pandas as pd

from pyspark.sql.functions import lit,trim,row_number

#from pyspark.sql.functions import col, trim, encode, format_number, format_string

import common as cm
import gestioncargue
import hdfs
import utils
import etl_Efectivo as ef


Carpeta_local = os.path.dirname(os.path.abspath(__file__))+ "/temp_etl"

def cargar(Hc,sigla, periodo):
    """
    Cargar un anexo
    :param Hc: contexto de hive
    :param sigla: nombre del anexo sigle de la tabla tproceso
    :param periodo: periodo a cargar
    :return: registra en la tlabla tcarge
    """
    fecha_inicio=datetime.now()
    sql ="(select * from get_proceso('%s')) as proceso" % (sigla)

    cm.verbose_c(sql,3)

    df=cm.get_Query_Postgresql(Hc,sql)
    
    # MARCAMOS EL INICIO general DE LA EJECUCION
    idproceso=df.select(df["idproceso"]).where("origen=='O'").collect()[0]['idproceso']

    idcargue_g=gestioncargue.marcar_inicio(Hc, idproceso,fecha_inicio)

    cm.verbose_c(idcargue_g,2)
<<<<<<< HEAD:ETL_paso1/etl.py
    cm.verbose_c(df.count(),2)
    #si el dataframe tiene dos registros en us cargue simple
    if df.count()==2:
        reg=carga_origen(Hc,df.select(df["idcatalogo"]).where("origen=='O'").collect()[0]['idcatalogo']\
                    ,df.select(df["nombrecatalogo"]).where("origen=='O'").collect()[0]['nombrecatalogo']\
                    ,df.select(df["tipocatalogo"]).where("origen=='O'").collect()[0]['tipocatalogo']\
                    ,df.select(df["basedatos"]).where("origen=='O'").collect()[0]['basedatos']\
                    ,df.select(df["separador"]).where("origen=='O'").collect()[0]['separador']\
                    ,df.select(df["nombrecatalogo"]).where("origen=='D'").collect()[0]['nombrecatalogo']\
                    ,df.select(df["idcatalogo"]).where("origen=='D'").collect()[0]['idcatalogo']\
                    ,df.select(df["basedatos"]).where("origen=='D'").collect()[0]['basedatos'] \
                    ,periodo,idproceso,idcargue_g)
    
    #Si el dataframe es un etl de Efectivo
    if df.count()==4:
        reg = ef.carga_origen(Hc, df, periodo, idproceso, idcargue_g)
    gestioncargue.marcar_fin_cargue_archivo(Hc, idcargue_g, 2, datetime.now(), reg,
=======

    #DEBEMOS BUSCAR CUANTOS PASOS TIENE E ITERAR POS PASOS

    pasos=df.select('paso').distinct()

    for paso in pasos.rdd.collect():



        reg=carga_origen(Hc,df.select(df["idcatalogo"]).where("origen=='O' and paso=" + str(paso['paso']) ).collect()[0]['idcatalogo']\
                    ,df.select(df["nombrecatalogo"]).where("origen=='O' and paso=" + str(paso['paso'])).collect()[0]['nombrecatalogo']\
                    ,df.select(df["tipocatalogo"]).where("origen=='O' and paso=" + str(paso['paso'])).collect()[0]['tipocatalogo']\
                    ,df.select(df["basedatos"]).where("origen=='O' and paso=" + str(paso['paso'])).collect()[0]['basedatos']\
                    ,df.select(df["separador"]).where("origen=='O' and paso=" + str(paso['paso'])).collect()[0]['separador'] \
                    ,df.select(df["idconexion"]).where("origen=='O' and paso=" + str(paso['paso'])).collect()[0]['idconexion'] \
                    ,df.select(df["servidor"]).where("origen=='O' and paso=" + str(paso['paso'])).collect()[0]['servidor'] \
                    ,df.select(df["nombrecatalogo"]).where("origen=='D' and paso=" + str(paso['paso'])).collect()[0]['nombrecatalogo']\
                    ,df.select(df["idcatalogo"]).where("origen=='D' and paso=" + str(paso['paso'])).collect()[0]['idcatalogo']\
                    ,df.select(df["basedatos"]).where("origen=='D' and paso=" + str(paso['paso'])).collect()[0]['basedatos'] \
                    ,periodo \
                    , df.select(df["idproceso"]).where("origen=='D' and paso=" + str(paso['paso'])).collect()[0]['idproceso'] \
                    ,idcargue_g)

    gestioncargue.marcar_fin( Hc, idcargue_g, 2, datetime.now(), reg,
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589:etl.py
                                            "Archivos cargados Correcatmente")



def carga_origen(Hc,idcatalogo_Or,catalogo_Or,tipocatalogo_Or,basedatos_Or,separador_Or,idconeccion_Or,servidor_Or, catalogo_Ds,idcatalogo_Ds,
                 basedatos_Ds, periodo,idproceso,idcargue_g):
    """
    Carga el grupo e archivos del anexo
    :param Hc: hive contex
    :param idcatalogo_Or: id del catalogo origen idcatalogo de la tabla tcatalogo
    :param catalogo_Or:nombre del catalogo origen campo Catalogo de la tabal tcatalogo
    :param tipocatalogo_Or: tipo del catalogo a cargar de tal tabla tipos catalogo
    :param basedatos_Or:base de datos u origen a cargar
    :param separador_Or:separador del archivo a cargar
    :param catalogo_Ds:nobre del catalogo destino
    :param idcatalogo_Ds: id del catalogo destino de lata tabla tcatalogo
    :param basedatos_Ds:base de datos donde se guardara la informacion
    :param periodo:Perido a cargar
    :param idproceso:proceso da la tabal tprocesos
    :param idcargue_g:marca dela tabla tcargue del proceso genral
    :return:nuemro total de registros cargados
    """
    total = 0
    if tipocatalogo_Or=='Archivo':
        if periodo=='0':
            meses = ('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12')
            periodo=str(date.today().year) + "/" + meses[date.today().month]

        origen= basedatos_Or +catalogo_Or +"/"+ str(periodo)+"/Validos/"
        tempo=Carpeta_local +'/' +catalogo_Or +"/"+ str(periodo) +"/"
        HDFS_TEMP_PATH = cm.GLOBAL_CONFIG.get("hdfs_paht_landing")  +'/' +catalogo_Or +"/"+ str(periodo) +"/"

        if  cm.GLOBAL_CONFIG.get("local"):
            basedatos_Ds=""
            HDFS_TEMP_PATH=tempo
        else:
            HDFS_TEMP_PATH = cm.GLOBAL_CONFIG.get("hdfs_paht_landing")  +'/' +catalogo_Or +"/"+ str(periodo) +"/"

        destino=origen.replace("Validos/", "Final/Procesados/")
        """
        try:
            os.makedirs(destino, exist_ok=True)
        except ImportError as error:
            print ("El archivo" +  str(error.output))
        """
        files = cm.files(origen)

        if (len(files) == 0):
            observaciones = "No existen  archivos para procesar en la carpeta " + origen
            cm.verbose_c(observaciones, 1)
            gestioncargue.marcar_fin(Hc, idcargue_g, 2, datetime.today(), 0, observaciones)
            exit(0)

<<<<<<< HEAD:ETL_paso1/etl.py
        mySchema = cm.get_schema(Hc, idcatalogo_Ds)
        total=0
=======
        mySchema = cm.get_shema(Hc, idcatalogo_Ds)

>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589:etl.py
        for file in files:

            if file[1].lower()=='txt'  or  file[1].lower()=='csv' :
                total= total + cargar_archivo(Hc,origen +file[0], idproceso,separador_Or,basedatos_Ds,catalogo_Ds,mySchema,HDFS_TEMP_PATH)

            else:
                if os.path.exists(tempo+file[0]):
                    os.remove(tempo+file[0])
                try:

                    shutil.unpack_archive(origen+file[0],tempo)

                except IOError as err:
                    observaciones = "Error decomprimiendo el archivo, %s, %s " % (origen+file[0] , format(err))
                    cm.verbose_c(observaciones, 1)
                    gestioncargue.marcar_fin(Hc, idcargue_g, 3, datetime.today(), 0, observaciones)
                    continue

                files_des = cm.files(tempo)
                for file_d in files_des:
                    total= total + cargar_archivo(Hc, tempo + file_d[0], idproceso, separador_Or, basedatos_Ds, catalogo_Ds, mySchema,HDFS_TEMP_PATH)
                    os.remove(tempo + file_d[0])
            shutil.copy(origen + file[0], destino)

    elif  tipocatalogo_Or=='Tabla':
        total = total + cargar_tabla (Hc,idcatalogo_Or,catalogo_Or,servidor_Or,idconeccion_Or, catalogo_Ds,idcatalogo_Ds,
                 basedatos_Ds,idproceso)

    return total


def cargar_archivo(Hc, archivo,idproceso,separador,basedatos,catalogo,schema,HDFS_TEMP_PATH):
    """
    -permite cargar un archivo txt a una base de datos
    :param Hc: hive contex
    :param archivo: ruta completa del archivo a cargar
    :param idproceso: id proceso de la tabla tproceso
    :param separador: separados del archivo
    :param basedatos: base de datos donde se gurdara la informacion
    :param catalogo: tabla donde se guardara la informacion
    :param schema:schema de la tabla a guardar
    :return: numero de registros cargados
    """
    observacion='Procesando Archivo ' + archivo
    cm.verbose_c(observacion, 0)
    fecha_cargue = datetime.now()

    idcargue = gestioncargue.marcar_inicio_cargue_archivo(Hc, idproceso, fecha_cargue,observacion)

    enc=utils.detectar_codec(archivo)
    
    cm.verbose_c('Cargue con HDFS ' + HDFS_TEMP_PATH+archivo[archivo.rfind("/")+1 :], 2)
    if  cm.GLOBAL_CONFIG.get("local"):
         shutil.copy(archivo,HDFS_TEMP_PATH+archivo[archivo.rfind("/")+1 :])
    else:
        hdfs.copyFromLocal(archivo,HDFS_TEMP_PATH)

<<<<<<< HEAD:ETL_paso1/etl.py
    dataset = leer_Archivo(Hc, HDFS_TEMP_PATH+archivo[archivo.rfind("/")+1 :], separador, schema, False, fecha_cargue, idcargue, enc)
=======
    hdfs.copyFromLocal(archivo,HDFS_TEMP_PATH)
    dataset = cm.leer_Archivo(Hc, HDFS_TEMP_PATH+archivo[archivo.rfind("/")+1 :], separador, schema, False, fecha_cargue, idcargue, enc)

>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589:etl.py
    regis=cm.save_df_hive(Hc,dataset,basedatos, catalogo,idcargue)

    if  not cm.GLOBAL_CONFIG.get("local"):
        hdfs.deletefileHDFS(HDFS_TEMP_PATH+archivo[archivo.rfind("/")+1 :])


    observacion = "Archivo " + archivo + " cargado correctamente"
    cm.verbose_c(observacion, 0)
    gestioncargue.marcar_fin_cargue_archivo(Hc, idcargue, 2, datetime.now(), regis,
                                            observacion)
    return regis

def cargar_tabla (Hc,idcatalogo_Or,catalogo_Or, servidor_Or,idconeccion_Or,catalogo_Ds,idcatalogo_Ds, \
                 basedatos_Ds,idproceso):

    #verifico si tiene parametros de cargue incremental

    fecha_cargue = datetime.now()

    idcargue = gestioncargue.marcar_inicio_cargue_archivo(Hc, idproceso, fecha_cargue, "PROCESO INICIADO")

    sql = "(select valor from public.tparametro where idproceso = %s and parametro = 'MAX_NUMERO_ENTREGA') as parametro" % str(idproceso)

    cm.verbose_c(sql, 3)
    df = cm.get_Query_Postgresql(Hc, sql)

    if df.count()>0:
        sql= "(select * from %s where NUMERO_ENTREGA >%s ) as data" %(catalogo_Or, str(df.select(df["valor"]).collect()[0]['valor']))
    else:
        sql="(select * from %s ) as data " %(catalogo_Or)

    cm.verbose_c(sql, 3)

    if servidor_Or  == "hadoop-hive":
        datos=Hc.sql(sql)
    else:
        datos = cm.get_Query_BaseER(Hc,sql,idconeccion_Or)

    cm.save_df_hive(Hc,datos,basedatos_Ds, catalogo_Ds,0)

    if df.count() > 0 and datos.count()>0:
        sql ="update  public.tparametro set valor=%s where idproceso = %s and parametro = 'MAX_NUMERO_ENTREGA'" \
             %(str(datos.groupby().max('NUMERO_ENTREGA').collect()[0]['max(NUMERO_ENTREGA)'] ),str(idproceso))
        cm.set_query_bd(sql, "postgresql-gestioncargues")

    regis=datos.count()

    observacion = "Tabla " + catalogo_Ds + " cargado correctamente"
    cm.verbose_c(observacion, 0)
    gestioncargue.marcar_fin_cargue_archivo(Hc, idcargue, 2, datetime.now(), regis,
                                            observacion)

    return regis

