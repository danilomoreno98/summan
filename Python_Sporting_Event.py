"""
**********************************************************
2021-11-10
Prueba técnica Summan
Elaborado por: Danilo Hernando Moreno Gonzalez
Script correspondiente a la tf de tabla Sporting_Event (Workshop Data Engineering Immmersion Day)
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
def tf_sporting_Event():
    # LLamado de variables globales
    global glueContext
    global sporting_Event

    # Aplicando tf tipo apply_mapping a columna start_date_time (TIMESTAMP) y start_date_time(DATE) según workshop
    sporting_Event = sporting_Event.apply_mapping([('id', 'bigint', 'id', 'bigint'), ('sport_type_name', 'string', 'sport_type_name', 'string'), ('home_team_id', 'bigint', 'home_team_id', 'bigint'), ('away_team_id', 'bigint', 'away_team_id', 'bigint'), ('location_id', 'bigint', 'location_id', 'bigint'), ('start_date_time', 'string', 'start_date_time', 'TIMESTAMP'),('start_date', 'string', 'start_date', 'DATE'),('sold_out', 'bigint', 'sold_out', 'bigint')])
    
"""
+++++++++++++++++++++++++++++++++++++++++++++++++
"""

"""
*************************************************
Definición de variables globales
*************************************************
"""
global glueContext
global sporting_Event
"""
+++++++++++++++++++++++++++++++++++++++++++++++++
"""

"""
*************************************************
Definición de variables locales y constantes
*************************************************
"""
# Inicializar variblas con nombre db y tablas a utilizar
db_name = "dbticketdata"     # Nombre de la base de datos
tbl_name = "sporting_Event"  # Nombre de la tabla a utilizar

# Directorios de salida en S3
output_s3_dir = 's3://testsumman/tickets/dms_parquet/sporting_event/'
"""
+++++++++++++++++++++++++++++++++++++++++++++++++
"""

#******************** MAIN ************************
# Inicializar el GlueContext
glueContext = GlueContext(SparkContext.getOrCreate())

# Creación de los Dynamic Frames de las tablas de origen (tabla = sporting_Event)
sporting_Event = glueContext.create_dynamic_frame.from_catalog(database=db_name, table_name=tbl_name)

#Generando tf a Tabla sporting_Event
tf_sporting_Event()

# Escribir el Dynamic Frame en el bucket de s3 con formato parquet
glueContext.write_dynamic_frame.from_options(frame = sporting_Event, connection_type = 's3', connection_options = {'path': output_s3_dir}, format = "parquet")