from extract_data import extract_data
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def data_transform():
    spark= SparkSession.builder.appName("Restaurant report").getOrCreate()
    df=spark.read.csv("E:/Project/data/intermediate/extracted_data.csv", header=True, inferSchema= True)

    
    df = df.filter(df.transaction_amount.isNull()).count()

    df= df.withColumn('total_cost', col('item_price')*col('quantity'))

    df= df.filter(df.total_cost == df.transaction_amount)

    final_df= df.select(df.transaction_amount.alias('trans_amount'),df.total_cost.alias('t-cost'),df.item_name.alias('name'))

    df.write.mode('overwrite').csv("E:/Project/data/intermediate/extracted_data2.csv", header=True)


    