# encoding=UTF-8
#-*- coding: UTF-8 -*-
import configparser
import os
import subprocess as sp
from pyspark.sql.types import *



GLOBAL_CONFIG = {
    "hdfs_paht_landing": "/landing/tmp/Principales/ARCHIVOS",
    "verbosity" : 0,
    "local" : 0
}

def get_Query_MSSQL(spC, jdbcHostname, jdbcPort, jdbcDatabase, username, password, pushdown_query):
    """
    Obtiene via jodbc el resultado de una consulta a una base de datos en servidor MS SQL
    :param spC: Spark contex
    :param jdbcHostname: Servidor del la BD
    :param jdbcPort: Puerto del motor
    :param jdbcDatabase: Base de datos
    :param username: Usuario de la BD
    :param password: Contrasela del usuario de la BD
    :param pushdown_query: Consulta a realizar
    :return: dataframe con  el resultado de la consulta
    """

    # jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, \
    #                                                                             username, password)
    jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

    connectionProperties = {
        "user": username,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    df = spC.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)

    return df

def get_BDproperties (driver):
    """
    Busca en el archivo .ini la configuracion de la bd segun el driver
    :param driver: driver a usar del .ini
    :return: propiedad de coneccion
    """
    db_properties = {}
    config = configparser.ConfigParser()
    config.read(os.path.dirname(os.path.abspath(__file__))+ '/CargueDWH.ini')
    db_prop = config[driver]
    #db_url = db_prop['url']
    db_properties['user'] = db_prop['username']
    db_properties['password'] = db_prop['password']
    #db_properties['url'] = db_prop['url']
    db_properties['driver'] = db_prop['driver']
    return db_properties

def get_BDurl (driver):
    """
    busca en el archivo .ini la url del servidor que vamos a consutar
    :param driver: driver del servidor que vamos a usar
    :return: url dleservidor
    """
    config = configparser.ConfigParser()
    config.read(os.path.dirname(os.path.abspath(__file__))+ '/CargueDWH.ini')
    db_prop = config[driver]
    db_url = db_prop['url']
    return db_url
def phat_driver (driver):
    config = configparser.ConfigParser()
    config.read('../CargueDWH.ini')
    return config[driver]['path_driver']
def get_Query_Postgresql(hC, pushdown_query):
    """
    Consulta una bd de posgres
    :param hC: hive contes
    :param pushdown_query: consulta sql
    :return: data frame con el reultado
    """

    jdbcUrl = get_BDurl ('postgresql')
    connectionProperties=get_BDproperties ('postgresql')
    #verbose_c(pushdown_query, 2)
    df = hC.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
    return df
    #optionally use jdbc url

def files(path):
    """
    :param path: Ruta
    :return: lista contodos los archivos
    """
    fi=[]
    if os.path.isdir(path):
        for file in os.listdir(path):
            if os.path.isfile(os.path.join(path, file)):
                 fi.append((file, (file[file.find('.') - len(file) + 1:])))
    return fi

def verbose_c( x , nivel ):
    """
    Imprime la cadena x dependiendo del nivel de la cadena y el aplicativo
    :param x: Cadena a imprimir
    :param nivel: nivel a plicar a la cadena
    :param verbose: nivel del aplicativo
    :return:
    """
    #print (GLOBAL_CONFIG["imper"])
    if GLOBAL_CONFIG["verbosity"] >= nivel:
        print ("======================================")
        print (x)
        print ("=======================================")


def save_pd_hive(Hc,dataset,database,table_name,mySchema,idcargue):
    sdf = Hc.createDataFrame(dataset,schema=mySchema)
    return save_df_hive(Hc,sdf,database,table_name,idcargue)


def save_df_hive(Hc,sdf,database, table_name,idcargue):
    verbose_c(sdf.columns, 3)
    #verbose_c(sdf.take(4), 3)
    if  not GLOBAL_CONFIG.get("local"):
        sdf.write.saveAsTable("%s.%s" % (database, table_name), mode="append")
        str_sql = "select count(1) as numero from %s.%s where idcargue=%s" % (database, table_name, idcargue)
    else:
        sdf.select(sdf.columns[:6],).show(30)
        sdf.write.saveAsTable("%s" % ( table_name), mode="append")
        str_sql = "select count(1) as numero from %s where idcargue=%s" % (table_name, idcargue)

    return (Hc.sql(str_sql).select("numero").collect()[0]["numero"])

def get_tipo(tipo):
    switcher = {
        'int': IntegerType(),
        'interger':IntegerType(),
        'varchar':StringType(),
        'string': StringType(),
        'float':FloatType(),
        'char':StringType(),
        'date':StringType(),
        'datetime':TimestampType(),
        'bigint':LongType()
    }
    return switcher.get(tipo.lower())

rt = []
def get_schema(Hc, idcatalogo):
    sql = "(select nombre,tipodato,longitud from tcampo where idcatalogo=%s order by orden) as tabla" % (idcatalogo)
    df = get_Query_Postgresql(Hc, sql)
    st = StructType()
    cols = []
    rt = []

    for  fila in df.rdd.collect():
        st.add(StructField(fila['nombre'].lower(), get_tipo(fila["tipodato"]), True))
        cols.append(fila['longitud'])


    rt.append(st)
    rt.append(cols)

    return rt

def kerberos_init(user, keytab):
        kinit_executable = "kinit"
        principal_name = user
        cmd = [
            kinit_executable,
            "-kt",
            keytab,
            principal_name
        ]
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT, shell=False)
        proc.wait()
        if proc.returncode != 0:
            print("--Unrecoverable error, Kerberos initialization failed")
            exit(1)
        else:
            print("--Kerberos initialization succesfull")

def table(Hc, origen, bdorigen):
    """
    Parameters
    ----------
    Hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    origen : TYPE: string
        DESCRIPTION: Tabla a la cual se verificara si existe o no.
    bdorigen : TYPE: string
        DESCRIPTION: Base de datos a la cual se le verificara su existencia.

    Returns
    -------
    existe : TYPE: Boolean
        DESCRIPTION: Existe o no existe.

    """
    sent = "select * from {}.{} limit 10".format(bdorigen, origen)
    try:
        df1 = Hc.sql(sent)
        verbose_c(df1.count(), 2)
        existe = True
    except:
        existe = False
    
    return existe
