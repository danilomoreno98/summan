"""
**********************************************************
2021-11-10
Prueba técnica Summan
Elaborado por: Danilo Hernando Moreno Gonzalez
Script correspondiente a la tf de tabla Sporting_Event_Ticket (Workshop Data Engineering Immmersion Day)
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
def tf_sporting_Event_Ticket():
    # LLamado de variables globales
    global glueContext
    global sporting_event_ticket_fr

    # Aplicando tf tipo apply_mapping a columnas id (DOUBLE), sporting_event_id (DOUBLE) y ticketholder_id(DOUBLE) según workshop
    sporting_event_ticket_fr = sporting_event_ticket_fr.apply_mapping([('id', 'string', 'id', 'double'), ('sporting_event_id', 'string', 'sporting_event_id', 'double'), ('sport_location_id', 'string', 'sport_location_id', 'string'), ('seat_level', 'string', 'seat_level', 'string'), ('seat_section', 'string', 'seat_section', 'string'), ('seat_row', 'string', 'seat_row', 'string'),('seat', 'string', 'seat', 'string'),('ticketholder_id', 'string', 'ticketholder_id', 'double'), ('ticket_price', 'double', 'ticket_price', 'double')])
    
"""
+++++++++++++++++++++++++++++++++++++++++++++++++
"""

"""
*************************************************
Definición de varibles globales
*************************************************
"""
global glueContext
global sporting_event_ticket_fr
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
tbl_name = "sporting_event_ticket"  # Nombre de la tabla a utilizar

# Directorios de salida en S3
output_s3_dir = 's3://testsumman/tickets/dms_parquet/sporting_event_ticket/'
"""
+++++++++++++++++++++++++++++++++++++++++++++++++
"""

#******************** MAIN ************************
# Inicializar el GlueContext
glueContext = GlueContext(SparkContext.getOrCreate())

# Creación de los Dynamic Frames de las tablas de origen (tabla = sporting_event_ticket_fr)
sporting_event_ticket_fr = glueContext.create_dynamic_frame.from_catalog(database=db_name, table_name=tbl_name)

#Generando tf a Tabla sporting_event_ticket_fr
tf_sporting_Event_Ticket()

# Escribir el Dynamic Frame en el bucket de s3 con formato parquet
glueContext.write_dynamic_frame.from_options(frame = sporting_event_ticket_fr, connection_type = 's3', connection_options = {'path': output_s3_dir}, format = "parquet")