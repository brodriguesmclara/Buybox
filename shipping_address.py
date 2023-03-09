import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql.functions import explode,udf
import pyspark.sql.functions as sf
from pyspark.sql.window import Window
from datetime import datetime, date


sc = pyspark.SQLContext(pyspark.SparkContext.getOrCreate())

print("Criando variaveis path")
input = "gs://buybox_discovery/historico/shipping_address.csv"
output = "gs://buybox_discovery/historico/shipping_address_output.parquet"

print("Criando schema")
schema = StructType() \
    .add("order_id",StringType(),True) \
    .add("given_name",StringType(),True) \
    .add("family_name",StringType(),True) \
    .add("street_name",StringType(),True) \
    .add("address_number",StringType(),True) \
    .add("complement",StringType(),True) \
    .add("district",StringType(),True) \
    .add("postal_code",StringType(),True) \
    .add("address_locality",StringType(),True) \
    .add("address_region",StringType(),True) \
    .add("locality_code",StringType(),True) \
    .add("reference_note",StringType(),True) \
    .add("updated_at",StringType(),True) \
    .add("created_at",StringType(),True) \
    .add("address_type",StringType(),True) \

print("Lendo arquivo de entrada")
#df_read = sc.read.format('CSV').option('header',False).option('delimiter','|').schema(schema).load(input)
df_read = sc.read.format('CSV').option('header',False).option('delimiter','|').option("encoding", "UTF-8").schema(schema).load(input)
print("Fim da leitura")

print("Criando arquivo de saida")
df_read.write.parquet(output)
print("Fim da execusao")


