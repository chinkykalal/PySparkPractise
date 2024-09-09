import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def data_extract():
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Define relative paths
    input_path = os.path.join(current_dir, '../data/raw')
    output_path = os.path.join(current_dir, '../data/extracted')
    print (input_path)
    print (output_path)
    spark= SparkSession.builder.appName("Restaurant report").getOrCreate()
    df=spark.read.csv(input_path, header=True, inferSchema= True)
    df.show()
    df.write.mode('overwrite').csv(output_path, header=True)


    

