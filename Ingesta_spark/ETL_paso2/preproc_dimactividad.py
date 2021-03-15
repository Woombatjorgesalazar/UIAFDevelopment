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

	if sigla == 'BDUDA':
		dfcruzar = preproc_adres(hc, dicc)

	return dfcruzar


def preproc_adres(hc, dicc):


	sent = "select * from dwh_uiaf.dimactividadeconimica where codigoactividad = 'Nore'"
	dfcruzar = hc.sql(sent)

	return dfcruzar

