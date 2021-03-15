#####Funciones quemadas para procesamiento de Efectivo####################
def stage_uiaf_efectivo(Hc, dataset):
    ####################################Funcion de parseo Efectivo#########################
    
    dataset.registerTempTable('dataset')
    dataset = Hc.sql("select trim(substring(_c0, 0, 10)) as Consecutivo, \
                     trim(substring(_c0, 11, 8)) as codigoentidad, \
                     trim(substring(_c0, 19, 10)) as Fecha_corte, \
                     trim(substring(_c0, 29, 10)) as Numero_registros  from dataset")
    dataset.registerTempTable('dataset2')
    dataset = Hc.sql("select * from dataset2 where consecutivo = '0'")
    dfp = dataset.toPandas()
    encabezado = dfp.head(1)
    #cm.verbose_c(encabezado, 2)
    
    sdfencabezado = Hc.createDataFrame(encabezado)
    
    dataset = Hc.sql("select trim(substring(_c0, 0, 10)) as Consecutivo, \
                     trim(substring(_c0, 11, 8)) as codigoentidad, \
                     trim(substring(_c0, 19, 10)) as Cantidad_Registros from dataset")
    dataset.registerTempTable('dataset2')
    dataset = Hc.sql("select * from dataset2 where Consecutivo = '0'")
    dfp = dataset.toPandas()
    pie = dfp.tail(1)
    
    sdfpie = Hc.createDataFrame(pie)
    
    #sdfencabezado.show()
    #sdfpie.show()
    
    dataset = Hc.sql("select trim(substring(_c0, 0, 10)) as Consecutivo, \
                     trim(substring(_c0, 11, 10)) as Fecha_transaccion, \
                     trim(substring(_c0, 21, 20)) as Valor_transaccion, \
                     trim(substring(_c0, 41, 3)) as Tipo_Moneda, \
                     trim(substring(_c0, 44, 15)) as Codigo_Oficina, \
                     trim(substring(_c0, 59, 2)) as Tipo_Producto, \
                     trim(substring(_c0, 61, 1)) as Tipo_Transaccion, \
                     trim(substring(_c0, 62, 1)) as Medio_Transaccion, \
                     trim(substring(_c0, 63, 20)) as Numero_cuenta, \
                     trim(substring(_c0, 83, 2)) as Tipo_Identificacion, \
                     trim(substring(_c0, 85, 20)) as Numero_Identificacion_titular, \
                     trim(substring(_c0, 105, 2)) as digito, \
                     trim(substring(_c0, 107, 255)) as Nombre_titular, \
                     trim(substring(_c0, 362, 5)) as cod_municipio, \
                     trim(substring(_c0, 367, 2)) as Tipo_Identificacion_Realiza, \
                     trim(substring(_c0, 369, 20)) as Numero_Identificacion_realiza, \
                     trim(substring(_c0, 389, 2)) as digito_realiza, \
                     trim(substring(_c0, 391, 255)) as Nombre_realiza from dataset")
    dataset.registerTempTable('dataset2')
    dataset = Hc.sql("select * from dataset2 where Consecutivo != '0'")
    
    return sdfencabezado, sdfpie, dataset



def ajuste_efectivo(Hc, head, pie, dataset):
    split_col = split(dataset['Nombre_titular'], ' ')
    split_col2 = split(dataset['Nombre_realiza'], ' ')
    
    
    head = head.withColumn('Numero_entrega', lit(None).cast(IntegerType())) \
            .withColumn('Consecutivo', col('Consecutivo').cast(IntegerType())) \
            .withColumn('Sector_Entidad', substring('codigoentidad', 0, 2).cast(IntegerType())) \
            .withColumn('Tipo_entidad', substring('codigoentidad', 3, 3).cast(IntegerType())) \
            .withColumn('Codigo_entidad', substring('codigoentidad', 6, 3).cast(IntegerType())) \
            .withColumn('Fecha_corte', col('Fecha_corte').cast(TimestampType())) \
            .withColumn('Numero_registros', col('Numero_registros').cast(IntegerType()))
            
    pie = pie.withColumn('Consecutivo', col('Consecutivo').cast(IntegerType())) \
            .withColumn('Sector_Entidad', substring('codigoentidad', 0, 2).cast(IntegerType())) \
            .withColumn('Tipo_entidad', substring('codigoentidad', 3, 3).cast(IntegerType())) \
            .withColumn('Codigo_entidad', substring('codigoentidad', 6, 3).cast(IntegerType())) \
            .withColumn('Cantidad_Registros', col('Cantidad_Registros').cast(IntegerType()))
            
    dataset = dataset.withColumn('Numero_entrega', lit(None).cast(IntegerType())) \
            .withColumn('Consecutivo', col('Consecutivo').cast(IntegerType())) \
            .withColumn('Fecha_transaccion', col('Fecha_transaccion').cast(TimestampType())) \
            .withColumn('Valor_transaccion', col('Valor_transaccion').cast(IntegerType())) \
            .withColumn('Tipo_Producto', col('Tipo_Producto').cast(IntegerType())) \
            .withColumn('Tipo_Transaccion', col('Tipo_Transaccion').cast(IntegerType())) \
            .withColumn('Medio_Transaccion', col('Medio_Transaccion').cast(IntegerType())) \
            .withColumn('Tipo_Identificacion_titular', col('Tipo_Identificacion').cast(IntegerType())) \
            .withColumn('Numero_Identificacion_titular', col('Numero_Identificacion_titular').cast(IntegerType())) \
            .withColumn('Primer_Nombre_Titular', split_col.getItem(0)) \
            .withColumn('Otros_Nombres_Titular', split_col.getItem((1))) \
            .withColumn('Primer_Apellido_Titular', split_col.getItem((2))) \
            .withColumn('Segundo_Apellido_Titular', split_col.getItem((3))) \
            .withColumnRenamed('Nombre_titular', 'Razon_social') \
            .withColumn('Codigo_Departamento_Municipio', col('cod_municipio').cast(IntegerType())) \
            .withColumn('Tipo_Identificacion_Realiza', col('Tipo_Identificacion_Realiza').cast(IntegerType())) \
            .withColumn('Primer_Nombre_Realiza', split_col2.getItem(0)) \
            .withColumn('Otros_Nombres_Realiza', split_col2.getItem(1)) \
            .withColumn('Primer_Apellido_Realiza', split_col2.getItem(2)) \
            .withColumn('Segundo_Apellido_Realiza', split_col2.getItem(3))
                
    dataset = dataset.drop('digito_realiza').drop('digito').drop('Nombre_realiza').drop('Tipo_Identificacion').drop('cod_municipio')
    pie = pie.drop('codigoentidad')
    head = head.drop('codigoentidad')
    return head, pie, dataset

