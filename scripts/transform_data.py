import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from .extract_data import data_extract

def data_transform():

    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Define relative paths
    input_path = os.path.join(current_dir, '../data/extracted')
    output_path = os.path.join(current_dir, '../data/transformed')
    spark= SparkSession.builder.appName("Restaurant report").getOrCreate()
    df=spark.read.csv(input_path, header=True, inferSchema= True)
    df.show()
    df.printSchema() 
   
    df= df.withColumn('total_cost', col('item_price')*col('quantity'))

    df= df.filter(df.total_cost == df.transaction_amount)
    final_df= df.select(df.transaction_amount.alias('trans_amount'),df.total_cost.alias('t-cost'),df.item_name.alias('name'))

    df.write.mode('overwrite').csv(output_path, header=True)


    
