from pyspark import SparkContext, SparkConf,HiveContext
import common as cm
import os
import findspark
import configparser
import utils


# Instanciar la configuración de Spark
sconf = SparkConf() \
    .setAppName("ETL Cargue DWH_HDFS ") \
    .set("spark.jars", "/opt/cloudera/parcels/CDH/jars/postgresql-42.2.5.jar")
    
#'tmp/landing/driver'
#'/opt/cloudera/parcels/CDH/jars'

# Crear el contexto para la aplicación Spark
sC = SparkContext(conf=sconf)

# Establecer el nivel del log de la aplicación
sC.setLogLevel('ERROR')
# Crear el context para Hive
Hc = HiveContext(sC)






def related_tables(hc, database = 'default', table_name='empty', id_catalog='empty', campos='empty'):
    """
    Parameters
    ----------
    hc : HiveContext
        Contexto de hiveSQL.
    database : str
        Nombre de la base de datos a importar de hive. The default is 'default'.
    table_name : str
        Nombre de la tabla a importar de Hive. The default is 'empty'.
    id_catalog : str
        Id de la tabla a importar de Hive. The default is 'empty'.
    campos : str, list
        Lista de los campos a importar de hive o cadena de empty. The default is 'empty'.

    Returns
    -------
    df : Dataframe de Pyspark
        Dataframe con los campos seleccionados de Pyspark.

    """
    if (table_name!='empty' and id_catalog=='empty'):
        query = 'select * from ' + database + '.' +table_name
    elif (table_name=='empty' and id_catalog!='empty'):
        query = "(select nombrecatalogo from tcatalogo where idcatalogo = '{}') as consult".format(id_catalog)
        df = cm.get_Query_Postgresql(hc, query)
        dfp = df.toPandas()
        table_name = str(dfp['nombrecatalogo'][0])
        query = 'select * from ' + database + '.' +table_name
    elif (table_name!='empty' and id_catalog!='empty'):
        query = 'select * from ' + database + '.' +table_name
    else:
        print('No params')
        
    df_1 = hc.sql(query)    
    
    if campos == 'empty':
        df = df_1
    else:
        df = df_1.select(campos)
    return df

###Ejemplo
#related_tables(Hc, id_catalog='64', database='dwh_uiaf').show()

######################Cruze de tablas sencillo#####################
def formateo_sent_sql(campos):
    """
    Parameters
    ----------
    campos : list
        Lista con los campos a cruzar en una lista.
    
    Returns
    -------
    sent : str
        Cadena con la sentencia SQL a consultar.

    """
    cad = 't1.{0} = t2.{1}'
    cad2 = 't1.{0} is null'
    cadena = ''
    cadena2 = ''
    for index,i in enumerate(campos):
        if index == 0:
            cadena += '('
            cadena += cad.format(i, i)
            cadena2 += cad2.format(i)
            if i == campos[-1]:
                cadena += ')'
        elif i == campos[-1]:
            cadena += ' and '
            cadena += cad.format(i, i)
            cadena += ')'
            cadena2 += ' and '
            cadena2 += cad2.format(i)
        else:
            cadena += ' and '
            cadena += cad.format(i, i)
            cadena2 += ' and '
            cadena2 += cad2.format(i)

    
    print(cadena)

    sent = """select t2.* 
            from tabla1 t1 right join tabla2 t2 on {0}
            where {1}""".format(cadena, cadena2)
    print(sent)
    return sent
##############Ejemplo
#campos = ['idpersona', 'iddane']
#formateo_sent_sql(campos)

def cruze_sencillo(hc, dfprincipal, dfcruzar, campos_cruzar):
        """
        Parameters
        ----------
        hc : HiveContext
            Contexto de hiveSQL.
        dfprincipal : Dataframe de pyspark
            Dataframe principal a cruzar.
        dfcruzar : Dataframe de pyspark
            Dataframe secundario a cruzar.
        campos_cruzar : list
            Lista de los campos a cruzar con un right join.
    
        Returns
        -------
        df : Dataframe de Pyspark
            Dataframe con los registros que no existen en el dataframe principal.
    
        """
        dfprincipal.registerTempTable('tabla1')
        dfcruzar.registerTempTable('tabla2')
        sent = formateo_sent_sql(campos_cruzar)
        print(sent)
        ####################Lanzador de consultas de hive###################
        df = hc.sql(sent)
        return df
"""
#########################Creando test de prueba#####################
dfprincipal = Hc.sql('select * from dwh_uiaf.dimpersona where dimpersona.idpersona != "1"')
#dfprincipal.show(10)

dfcruzar = Hc.sql('select * from dwh_uiaf.dimpersona where dimpersona.idpersona = "1" or dimpersona.idpersona = "2"')   
############Lanzar funcion####################
campos = ['idpersona', 'identificacion']
cruze_sencillo(Hc, dfprincipal, dfcruzar, campos).show()
"""

def cruze_tablas(hc, tabla1, tabla2):
    """
    Parameters
    ----------
    hc : HiveContext
        Contexto de hiveSQL.
    tabla1 : str
        Cadena del nombre o id_catalogo de la tabla 1.
    tabla2 : str
        Cadena del nombre o id_catalogo de la tabla 2.
    

    Returns
    -------
    dff : Dataframe de Pyspark
        Dataframe con los campos que se cruzan.

    """
    tabla1 = str(tabla1)
    tabla2 = str(tabla2)
    if tabla1.isdigit():
        query = "(select nombrecatalogo from tcatalogo where idcatalogo = '{}') as consult".format(tabla1)
        df = cm.get_Query_Postgresql(hc, query)
        dfp = df.toPandas()
        tabla1 = str(dfp['nombrecatalogo'][0])
    if tabla2.isdigit():
        query = "(select nombrecatalogo from tcatalogo where idcatalogo = '{}') as consult".format(tabla2)
        df = cm.get_Query_Postgresql(hc, query)
        dfp = df.toPandas()
        tabla2 = str(dfp['nombrecatalogo'][0])
        
    consulta = "(select * from cruze_tablas('{}','{}')) as consult".format(tabla1,tabla2)
    dff = cm.get_Query_Postgresql(hc, consulta)
    
    print(tabla1,tabla2, '######Estas son las tablas')
    return dff

#cruze_tablas(Hc, '71', '72').show()



















