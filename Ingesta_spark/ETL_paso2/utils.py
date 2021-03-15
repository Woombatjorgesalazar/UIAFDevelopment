# encoding=UTF-8
#-*- coding: UTF-8 -*-
from chardet.universaldetector import UniversalDetector
import common as cm

def detectar_codec(Archivo):
    detector = UniversalDetector()
    file = open(Archivo, 'rb')
    if  cm.GLOBAL_CONFIG.get("local"):
        S=100
    else:
        S=10000

    i =0
    for line in file:
        detector.feed(line)
        if detector.done: break
        i += 1

        if i>S: break
    result =detector.close()
    file.close()
    return result


def find (list, value):
    try:
        list.index(value)
        return True
    except:
        return False



def last_idcargue(hc, tabla):
    """
    Parameters
    ----------
    Hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    tabla : TYPE: String
        DESCRIPTION: Tabla de stage a la que se le quiere consultar el ultimo id.
    

    Returns
    -------
    lidcargue : TYPE: String
        DESCRIPTION: Ultimo idcargue de la tabla stage a cargar por paso 2.

    """
    sent = "(select * from result_cargue('{0}', 'tabla')) as idcargue".format(tabla)
    df = cm.get_Query_Postgresql(hc, sent).toPandas()

    try:
        lidcargue = df['idcargue'][0]
    except:
        lidcargue = '0'

    return lidcargue

