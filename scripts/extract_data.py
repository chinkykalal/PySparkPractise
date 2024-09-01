from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def data_extract():
    spark= SparkSession.builder.appName("Restaurant report").getOrCreate()
    df=spark.read.csv("E:/Project/data/raw/Balaji_Fast_Food_Sales.csv", header=True, inferSchema= True)
    df.write.mode('overwrite').csv("E:/Project/data/intermediate/extracted_data.csv", header=True)

    

