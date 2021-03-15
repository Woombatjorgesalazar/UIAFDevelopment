## Versión de ETLs basado en PySpark

Generalidades

El proyecto se encuentra organizado por módulos cargados en la tabla tproceso y tcatalogo; en tproceso consignacmos los 
pásos a ejecutar y en tcatalogo los origenes y destinos un ETL debe tener como minimop un oriogen y un destino 

##Creacion del entorno de ejecución
En la tabla tsistema encontramos el servidor y la bd o fuente de informacion (puede ser una ruta en caso de ser un  archivo) 
podremos ejecutar el procedimiento get_proceso('BDUDA') con la sigla del proceso de tproceso lo que nos devolvera la informacion 
de los pasoso y origenes de informacion  

##Arquitectura del proyecto

El proyecto consta de dos archivos centrales: etl.py y main.py.  

* etl.py : Define las funciones genéricas para todos los archivos ETL. Tales funciones permiten procesar archivos de ancho fijo, cruzar fuentes de datos, gestionar la 
manera sobre cómo los datos se almacenan en el Clúster.

* main.py: Es el punto de entrada central de la ejecución. Contiene parámetros de entorno para configurar la sesión de Spark y la ejecución de archivos ETL a demanda.

## Ejecución de los ETLs

Para ejecutar los ETLs, se requiere construir una instrucción con las siguiente estructura: 

spark-submit main.py [OPTION]...
*   -p -periodo [yyyy/mm] periodo de carga
*   -m -modulo  [modulo] modulo a cargar
*   -v  niveL de verbosity deseado sin el argumento para nada y hasta -vvvv para todo

Por ejemplo para ejecutar el reporte de productos general de superfinanciera la instrucción debe ser la siguiente:

spark-submit --jars /opt/cloudera/parcels/CDH/jars/postgresql-42.2.5.jar main.py -m BDUDA -p 2019/04 

##Gestion de cargue 

en la tabla tcargue encontrara el resultado de cargue de cada paso del proceso


 ##Anulación de reportes

La tabla "fact_registro_reportes" que contiene los registros del procesamiento de reportes en el Clúster contiene la siguiente estructura:
* id_registro  : Se refiere al ID generado por SIREL cuando se carga el archivo.
* nombre_archivo : Nombre del archivo original
* ruta_ completa_archivo : Ruta completa del archivo original
* contenido_zip : En caso de un archivo comprimido, se lista el contenido.
* fecha_procesamiento: Fecha en la que se procesó el archivo.
* anexo_técnico: Sigla del anexo técnico del archivo cargado.
* reporte_anulado : Marca que indica S (si el reporte está anulado) N (el reporte no está anulado)
* duracion: Duración del proceso de cargue

