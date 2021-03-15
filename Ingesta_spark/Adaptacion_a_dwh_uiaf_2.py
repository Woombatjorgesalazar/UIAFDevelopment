from pyspark import SparkContext, HiveContext, SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql.functions import udf
from pyspark.sql.window import Window
from pyspark.sql.functions import lit
import common as cm
import utils
from pyspark.sql.functions import *
import time
import tareas_2 as t2
import tareas_1 as t1
import tareas_2_Efectivo as ii
import re




sc = SparkContext()
hc = HiveContext(sc)

'''create table dwh_uiaf2.dimactividadeconomicadetallado as select *
from dwh_uiaf.dimactividadeconomicasistema;'''

tablas = hc.sql('show tables from dwh_uiaf').toPandas()

create = 'create table dwh_uiaf2.{0} as {1}'
cad = 'select {0} from dwh_uiaf.{1}'
nn = '{0} as {1} ,'
sen = ''

for index,i in enumerate(tablas['tableName']):
    if re.search('dim', i) and re.search('sistema', i) :
        
        sent = 'describe dwh_uiaf.{0}'.format(i)
        desc = hc.sql(sent).toPandas()
        for index2, x in enumerate(desc['col_name']):
            if re.search('idsistema',x) or re.search('identificacionsistema', x) or re.search('identificacionentidadsistema', x):
                sen += nn.format(x, x)    
            elif x[:2] == 'id' and re.search('sistema', x):
                sen += nn.format(x, x.replace('sistema', 'detallado'))
            else:
                sen += nn.format(x, x)
        

        sent = cad.format(sen[:-2], i)
        detll = i.replace('sistema','detallado')
        sent = create.format(detll, sent)
        
        table = hc.sql(sent)
        table.show()
        sen = ''

        
    else:

        sent = 'describe dwh_uiaf.{0}'.format(i)
        desc = hc.sql(sent).toPandas()
        for index2, x in enumerate(desc['col_name']):
            sen += nn.format(x, x)

        sent = cad.format(sen[:-2], i)
        detll = i
        sent = create.format(detll, sent)
        
        table = hc.sql(sent)
        table.show()
        sen = ''






exit(0)





"""
create table dwh_uiaf2.dimactividadeconomica as select * from dwh_uiaf.dimactividadeconimica;
create table dwh_uiaf2.dimactividadeconomicadetallado as select *
from dwh_uiaf.dimactividadeconomicasistema;"""


