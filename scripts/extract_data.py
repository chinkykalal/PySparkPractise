import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def data_extract():
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Define relative paths
    input_path_supply_chain1 = os.path.join(current_dir, '../data/raw/supply_chain_data.csv')
    input_path_supply_chain2 = os.path.join(current_dir, '../data/raw/supply_chain_data2.csv')
    output_path1 = os.path.join(current_dir, '../data/extracted/supply_chain_data')
    output_path2= os.path.join(current_dir, '../data/extracted/supply_chain_data2')

    spark= SparkSession.builder.appName("customer sales").getOrCreate()
    df1=spark.read.csv(input_path_supply_chain1, header=True, inferSchema= True, sep="\t")
    df2=spark.read.csv(input_path_supply_chain2, header=True, inferSchema= True, sep="\t")
    df1.show()
    df2.show()
    print("before-------------------- PARQUET")
    df1 = df1.coalesce(1)
    # df2 = df2.coalesce(1)
    df1.write.mode('overwrite').parquet(output_path1)
    df2.write.mode('overwrite').parquet(output_path2)
    print("After-------------------- PARQUET")



    

