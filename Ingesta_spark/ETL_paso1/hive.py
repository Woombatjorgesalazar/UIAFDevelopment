# encoding=UTF-8
#-*- coding: UTF-8 -*-

import pandas as pd
import os

try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import HiveContext
    from pyspark.sql import SQLContext
    from pyspark.sql.types import *
except ImportError as error:
    print ('Cannot import Pyspark modules')
    raise
"""os.environ["PYSPARK_PYTHON"]='/opt/cloudera/parcels/Anaconda-2019.03/bin/python'
os.environ["PYSPARK_DRIVER_PYTHON"]='/opt/cloudera/parcels/Anaconda-2019.03/bin/python'
"""
sc = SparkContext()
hiveCtx = HiveContext(sc)
def savetohive(dataset,database,table_name,mySchema):
    sdf = hiveCtx.createDataFrame(dataset,schema=mySchema)
    sdf.write.saveAsTable("%s.%s" % (database, table_name), mode="append")


mySchema = StructType([StructField("DATAID",IntegerType() ,True)\
                            ,StructField("VERSIONNUM",IntegerType(),True)\
                            ,StructField("FIRST_NAME",StringType(),True)\
                            ,StructField("SECOND_NAME",StringType(),True)\
                            ,StructField("THIRD_NAME",StringType(),True)\
                            ,StructField("FOURTH_NAME",StringType(),True)\
                            ,StructField("UN_LIST_TYPE",StringType(),True)\
                            ,StructField("REFERENCE_NUMBER",StringType(),True)\
                            ,StructField("LISTED_ON",StringType(),True)\
                            ,StructField("GENDER",StringType(),True)\
                            ,StructField("SUBMITTED_BY",StringType(),True)\
                            ,StructField("NAME_ORIGINAL_SCRIPT",StringType(),True)\
                            ,StructField("COMMENTS1",StringType(),True)\
                            ,StructField("NATIONALITY2",StringType(),True)\
                            ,StructField("SORT_KEY",StringType(),True)\
                            ,StructField("SORT_KEY_LAST_MOD",StringType(),True)\
                            ,StructField("DELISTED_ON",StringType(),True)\
                            ,StructField("LIST_TYPE",StringType(),True)\
                            ,StructField("NATIONALITY",StringType(),True)\
                            ,StructField("LAST_DATE_UPDATED",StringType(),True)\
                            ,StructField("TITLE",StringType(),True)\
                            ,StructField("DESIGNATION",StringType(),True)\
                            ,StructField("ALIAS_QUALITY",StringType(),True)\
                            ,StructField("ALIAS_NAME",StringType(),True)\
                            ,StructField("ALIAS_DATE_OF_BIRTH",StringType(),True)\
                            ,StructField("ALIAS_CITY_OF_BIRTH",StringType(),True)\
                            ,StructField("ALIAS_COUNTRY_OF_BIRTH",StringType(),True)\
                            ,StructField("ALIAS_NOTE",StringType(),True)\
                            ,StructField("ADDRESS_STREET",StringType(),True)\
                            ,StructField("ADDRESS_CITY",StringType(),True)\
                            ,StructField("ADDRESS_STATE_PROVINCE",StringType(),True)\
                            ,StructField("ADDRESS_ZIP_CODE",StringType(),True)\
                            ,StructField("ADDRESS_COUNTRY",StringType(),True)\
                            ,StructField("ADDRESS_NOTE",StringType(),True)\
                            ,StructField("DOB_TYPE_OF_DATE",StringType(),True)\
                            ,StructField("DOB_DATE",StringType(),True)\
                            ,StructField("DOB_YEAR",FloatType(),True)\
                            ,StructField("DOB_FROM_YEAR",FloatType(),True)\
                            ,StructField("DOB_TO_YEAR",FloatType(),True)\
                            ,StructField("DOB_NOTE",StringType(),True)\
                            ,StructField("POB_CITY",StringType(),True)\
                            ,StructField("POB_STATE_PROVINCE",StringType(),True)\
                            ,StructField("POB_COUNTRY",StringType(),True)\
                            ,StructField("POB_NOTE",StringType(),True)\
                            ,StructField("DOC_TYPE_OF_DOCUMENT",StringType(),True)\
                            ,StructField("DOC_TYPE_OF_DOCUMENT2",StringType(),True)\
                            ,StructField("DOC_NUMBER",StringType(),True)\
                            ,StructField("DOC_ISSUING_COUNTRY",StringType(),True)\
                            ,StructField("DOC_DATE_OF_ISSUE",StringType(),True)\
                            ,StructField("DOC_CITY_OF_ISSUE",StringType(),True)\
                            ,StructField("DOC_COUNTRY_OF_ISSUE",StringType(),True)\
                            ,StructField("DOC_NOTE",StringType(),True)])

dataset = pd.read_csv("/opt/listas/onu/individual.csv",delimiter=';')
savetohive(dataset,'default','onu_individuos',mySchema)
