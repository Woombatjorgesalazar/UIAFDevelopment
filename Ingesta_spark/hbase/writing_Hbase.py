
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import datetime
import os

os.system('echo "Q1w2e3r4t5" | kinit woombatcg')

sc = SparkContext()
sqlc = SQLContext(sc)

sc.setLogLevel("WARN")

print('Lectura Inicial')

df = sqlc.read \
  .format("org.apache.phoenix.spark") \
  .option("table", "TABLE1") \
  .option("zkUrl", "localhost:2181") \
  .load()

print('DataFrame Creado')

Valor = df.select('ID').collect()[-1]
val = int(Valor['ID']) + 1

now = datetime.datetime.now()
Hora = str(now.year) + '-' + str(now.month)  + '-' + str(now.day) + '::' + str(now.hour)  + ':' +  str(now.minute) + ':' + str(now.second)

cSchema = StructType([StructField("ID", IntegerType())\
                      ,StructField("COL1", StringType())])

test_list = [[val, Hora]]

print('Registro a escribir', test_list)

df1 = sqlc.createDataFrame(test_list,schema=cSchema) 

print('Escribiendo')

df1.write \
  .format("org.apache.phoenix.spark") \
  .mode("overwrite") \
  .option("table", "TABLE1") \
  .option("zkUrl", "localhost:2181") \
  .save()

df = sqlc.read \
  .format("org.apache.phoenix.spark") \
  .option("table", "TABLE1") \
  .option("zkUrl", "localhost:2181") \
  .load()

print('Lectura Final')

df.show()


