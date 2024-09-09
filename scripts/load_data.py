import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.transform_data import data_transform
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def data_load():
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Define relative paths
    input_path = os.path.join(current_dir, '../data/transformed')
    output_path = os.path.join(current_dir, '../data/load')
    spark= SparkSession.builder.appName("Restaurant report").getOrCreate()
    df=spark.read.csv(input_path, header=True, inferSchema= True)
    df.write.mode('overwrite').csv(output_path)
   
