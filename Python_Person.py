"""
**********************************************************
2021-11-10
Prueba técnica Summan
Elaborado por: Danilo Hernando Moreno Gonzalez
Script correspondiente a la tf de tabla Person (Workshop Data Engineering Immmersion Day)
**********************************************************
"""

# Importar librerias para utilizar herramientas de AWS Glue y pyspark
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


"""
*************************************************
Definición de métodos
*************************************************
"""
def tf_person():
    # LLamado de variables globales
    global glueContext
    global person_fr

    # Aplicando tf tipo apply_mapping a columnas id (DOUBLE), sporting_event_id (DOUBLE) y ticketholder_id(DOUBLE) según workshop
    person_fr = person_fr.apply_mapping([('id', 'string', 'id', 'double'),('full_name', 'string', 'full_name', 'string'),('last_name', 'string', 'last_name', 'string'),('first_name', 'string', 'first_name', 'string')])
    
"""
+++++++++++++++++++++++++++++++++++++++++++++++++
"""

"""
*************************************************
Definición de varibles globales
*************************************************
"""
global glueContext
global person_fr
"""
+++++++++++++++++++++++++++++++++++++++++++++++++
"""

"""
*************************************************
Definición de varibles locales y constantes
*************************************************
"""
# Inicializar variblas con nombre db y tablas a utilizar
db_name = "dbticketdata"     # Nombre de la base de datos
tbl_name = "person"  # Nombre de la tabla a utilizar

# Directorios de salida en S3
output_s3_dir = 's3://testsumman/tickets/dms_parquet/person/'
"""
+++++++++++++++++++++++++++++++++++++++++++++++++
"""

#******************** MAIN ************************
# Inicializar el GlueContext
glueContext = GlueContext(SparkContext.getOrCreate())

# Creación de los Dynamic Frames de las tablas de origen (tabla = person)
person_fr = glueContext.create_dynamic_frame.from_catalog(database=db_name, table_name=tbl_name)

#Generando tf a Tabla person
tf_person()

# Escribir el Dynamic Frame en el bucket de s3 con formato parquet
glueContext.write_dynamic_frame.from_options(frame = person_fr, connection_type = 's3', connection_options = {'path': output_s3_dir}, format = "parquet")