"""
**********************************************************
Prueba técnica Summan
Elaborado por: Danilo Moreno
Script correspondiente a la tf de tabla Sport_location (Workshop Data Engineering Immmersion Day)
**********************************************************
"""

# Importar librerias para utilizar herramientas de AWS Glue
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Inicializar el GlueContext
glueContext = GlueContext(SparkContext.getOrCreate())

# Inicializar variblas con nombre db y tablas a utilizar
db_name = "dbticketdata"     # Nombre de la base de datos
tbl_name = "sport_location"  # Nombre de la tabla a utilizar

# Directorios de salida enS3
output_s3_dir = 's3://testsumman/tickets/dms_parquet/sport_location/'

# Creación de los Dynamic Frames de las tablas de origen (tabla = sport_location)
sport_location = glueContext.create_dynamic_frame.from_catalog(database=db_name, table_name=tbl_name)

# Escribir el Dynamic Frame en el bucket de s3 con formato parquet
glueContext.write_dynamic_frame.from_options(frame = sport_location, connection_type = 's3', connection_options = {'path': output_s3_dir}, format = "parquet")