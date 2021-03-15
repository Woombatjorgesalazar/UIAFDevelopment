from pyspark import SparkContext, HiveContext, SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
from datetime import datetime,date
import common as cm
import utils
from pyspark.sql.functions import *
import time
import tareas_2_Efectivo as t2
import tareas_1_Efectivo as t1
import prepro_efectivo as pre
import gestioncargue
import etl_Efectivo as ef

actual = time.strftime("20%y-%m-%d %H:%M:%S")
actualsin = time.strftime("20%y-%m-%d")
actualsin
#######################Contextos de spark y hive################
sc = SparkContext()
hc = HiveContext(sc)




hc.sql('select * from dwh_uiaf2.dimpersonadetallado').show()




















