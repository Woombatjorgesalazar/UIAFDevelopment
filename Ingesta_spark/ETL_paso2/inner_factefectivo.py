from pyspark import SparkContext, HiveContext, SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType
from pyspark.sql.functions import desc, row_number, monotonically_increasing_id
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
import common as cm
import utils
from pyspark.sql.functions import *
import time
import cruze_inner as ci





def clasificador(hc, dfcruzar, dicc, sigla):
	"""
    Parameters
    ----------
    hc : TYPE: class 'pyspark.sql.context.HiveContext'
        DESCRIPTION: Contexto de hive.
    dfcruzar : TYPE: Dataframe de pyspark.
        DESCRIPTION: Dataframe a cruzar con tabla de dwh_uiaf.
    dicc : TYPE: dict
        DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.
    sigla : TYPE: String
        DESCRIPTION: Palabra clave acerca de el origen de las tablas a cargar.
    

    Returns
    -------
    df : TYPE: Dataframe de pyspark
        DESCRIPTION: Tabla resultado inicial a cargar a dwh_uiaf.

    """
	if sigla == 'TE10':
	    df = cruze_efectivo(hc, dfcruzar, dicc)


	return df



def cruze_efectivo(hc, dfcruzar, dicccruzar):
	idsistema = dicccruzar['idsistema']
	idcatalogo = dicccruzar['idcatalogo']
	database = 'dwh_uiaf2'
	dfdestino = 'factefectivo'
	namedfprincipal = 'factefectivo'
	col_inc = 'idefectivo'
	adc = ''
	inc = ci.incremental(hc, database, dfdestino, col_inc)


	df, dfpt = ci.cruze_en_detalle(hc, dfcruzar, dicccruzar, database, namedfprincipal, idsistema, idcatalogo, adc)

	df.show()
	df.printSchema()
	exit(0)
	



