import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# --- Inicialización estándar de Glue ---
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# 1. LEO DATOS DE QUALITY

# --- MÉTODO ORIGINAL (Lo comentamos para la prueba) ---
# dyf = glueContext.create_dynamic_frame.from_catalog(
#     database="demo-aws",
#     table_name="dim_sucursales"
# )

# --- MÉTODO DE PRUEBA (Leer directo de S3) ---
# ¡IMPORTANTE! Asegúrate que esta sea la ruta "Location"
# exacta que viste en tu tabla de Glue.
s3_path = "s3://demo-cloud-quality/dim_sucursales/" 

print(f"PRUEBA: Leyendo directamente desde S3: {s3_path}")

dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [s3_path]},
    format="parquet"
)

# --- El resto del script sigue igual ---

# Convierto el DynamicFrame a DataFrame de Spark
df = dyf.toDF()

# --- ¡PASO DE DEBUG CLAVE! ---
# Vemos qué esquema leyó Spark directamente de S3
print("================= ESQUEMA REAL LEYENDO DE S3 =================")
df.printSchema()
print("==============================================================")

# 2. SELECCIONO CAMPOS Y REMUEVO DUPLICADOS
# Esta línea fallará si el printSchema sigue mostrando "root"
df_localidades = df.select("sucursales_localidad", "sucursales_provincia", "sk_provincia_localidad")
df_localidades_unicas = df_localidades.distinct()

# 4. GUARDAR EN QUALITY
s3_output_path = "s3://demo-cloud-quality/dim_localidad/"
df_localidades_unicas.write.mode("overwrite").parquet(s3_output_path)

job.commit()