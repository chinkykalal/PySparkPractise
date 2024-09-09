import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.transform_data import data_transform
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

output_path="E:/Project/data/processed/Balaji_Fast_Food_Sales.csv"

def data_load():
    spark= SparkSession.builder.appName("Restaurant report").getOrCreate()
    df=spark.read.csv("E:/Project/data/intermediate/extracted_data2.csv", header=True, inferSchema= True)
    df.write.mode('overwrite').csv(output_path)
   
