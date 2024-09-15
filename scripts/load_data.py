import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.transform_data import data_transform
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def data_load():
    # current_dir = os.path.dirname(os.path.abspath(__file__))

    # # Define relative paths
    # input_path = os.path.join(current_dir, '../data/transformed')
    # output_path = os.path.join(current_dir, '../data/load')
    # spark= SparkSession.builder.appName("Restaurant report").getOrCreate()
    # df=spark.read.csv(input_path)
    # # df.write.mode('overwrite').csv(output_path)
   

    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Define paths for input (where CSV files are stored) and output (where CSVs will be saved)
    input_path = os.path.join(current_dir, '../data/transformed')
    output_path = os.path.join(current_dir, '../data/load')

    # Initialize the Spark session
    spark = SparkSession.builder.appName("Restaurant report").getOrCreate()

    # List all CSV files in the transformed folder
    csv_files = [f for f in os.listdir(input_path) if f.endswith('.csv')]

    # Loop through the CSV files, read them as DataFrames, and save them to the load folder
    for file in csv_files:
        file_path = os.path.join(input_path, file)
        df_name = file.split('.')[0]  # Use the file name (without .csv) for processing
        
        # Read the CSV file into a DataFrame
        df = spark.read.option("header", "true").csv(file_path)
        
        # Define the output file path
        output_file_path = os.path.join(output_path, file)
        
        # Write the DataFrame back to the load directory as CSV
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_file_path)
        
        print(f"Read {file} and saved to {output_file_path}")
