from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, HiveContext, SparkSession
import os 
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /opt/cloudera/parcels/CDH/jars/postgresql-42.2.5.jar  pyspark-shell'


sparkConf = SparkConf().setAppName("App").set("spark.jars", "/opt/cloudera/parcels/CDH/jars/postgresql-42.2.5.jar")
sc = SparkContext(conf=sparkConf)
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc);


source_df = hiveContext.read.format('jdbc').option("url", "jdbc:postgresql://172.41.10.15:5432/gestioncargues") \
  .option("dbtable", "tproceso") \
  .option("user", "postgres") \
  .option("driver", "org.postgresql.Driver") \
  .option("password", "postgres").load()

source_df.show(10)



