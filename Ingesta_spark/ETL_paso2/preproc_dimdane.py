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






def clasificador(hc, dicc, sigla):
	"""
	Parameters
	----------
	hc : TYPE: class 'pyspark.sql.context.HiveContext'
		DESCRIPTION: Contexto de hive.
	dicc : TYPE: dict
		DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.
	sigla : TYPE: String
		DESCRIPTION: Palabra clave acerca de el origen de las tablas a cargar.

	Returns
	-------
	dfcruzar : TYPE: Dataframe de pyspark
		DESCRIPTION: Tabla a cruzar preprocesada de stage_uiaf.

	"""

	if sigla == 'TE10':
		dfcruzar = preproc_efectivo(hc, dicc)
	elif sigla == 'BDUDA':
		dfcruzar = preproc_adres(hc, dicc)

	return dfcruzar


def preproc_efectivo(hc, dicc):
	"""
	Parameters
	----------
	hc : TYPE: class 'pyspark.sql.context.HiveContext'
		DESCRIPTION: Contexto de hive.
	dicc : TYPE: dict
		DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.
	

	Returns
	-------
	dfcruzar : TYPE: Dataframe de pyspark
		DESCRIPTION: Tabla a cruzar preprocesada de stage_uiaf.

	"""
	idcargue = utils.last_idcargue(hc, 'TE10')
	idcargue = 157
	sent = "select * from stage_uiaf.te10_detalles where idcargue = {0}".format(str(idcargue))
	dfcruzar = hc.sql(sent)
	
	
	dfcruzar.registerTempTable("prueba")
	sent = "select if(length(cast(codigo_departamento_municipio as string)) = 4, \
		concat('0', cast(codigo_departamento_municipio as string)), \
		cast(codigo_departamento_municipio as string)) as iddane from prueba"
	dfcruzar = hc.sql(sent)
	dfcruzar = dfcruzar.groupBy('iddane').count()
	#dfcruzar.show()
	#dfcruzar.printSchema()
	#cm.verbose_c('Este es el final', 2)

	return dfcruzar

def preproc_adres(hc, dicc):
	"""
	Parameters
	----------
	hc : TYPE: class 'pyspark.sql.context.HiveContext'
		DESCRIPTION: Contexto de hive.
	dicc : TYPE: dict
		DESCRIPTION: Diccionario de campos, sistema, catalogo, preprocesamiento.

	Returns
	-------
	dfcruzar : TYPE: Dataframe de pyspark.
		DESCRIPTION: Dataframe preprocesado manualmente para su posterior cruze.

	"""
	
	idcargue = utils.last_idcargue(hc, 'ADRES_BDUDA')
	#idcargue = 152
	#############Seleccion y ajuste de acuerdo a la tabla de stage_uiaf################
	iddepartamento = 'nivelsisben'
	idmunicipio = 'coddepafi'
	sent = "select * from stage_uiaf.adres_bduda where idcargue = {0}".format(str(idcargue))
	dfcruzar = hc.sql(sent)
	dfcruzar.registerTempTable("prueba")
	dfcruzar = hc.sql("select concat(if(length({0}) = 1, concat('0',{0}), {0}), \
					  right(concat('000', {1}), 3)) as {2} from prueba" \
						.format(iddepartamento,idmunicipio, dicc['campos'][0]))
	dfcruzar = dfcruzar.groupBy('{0}'.format(dicc['campos'][0])).count()
		
	
	return dfcruzar






