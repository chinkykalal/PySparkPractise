import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from .extract_data import data_extract

def data_transform():

    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Define relative paths
    input_path1 = os.path.join(current_dir, '../data/extracted/supply_chain_data')
    input_path2 = os.path.join(current_dir, '../data/extracted/supply_chain_data2')

    output_path = os.path.join(current_dir, '../data/transformed')


    spark= SparkSession.builder.appName("customer sales").getOrCreate()
    df1=spark.read.parquet(input_path1)
    df2=spark.read.parquet(input_path2)

    # ------------------Data Cleaning and Preparation:------------------
    df2 = df2.withColumn("Return Rate",split(col("Return Rate"), "%").getItem(0).cast("float") / 100)
    df2 = df2.withColumn("Discount", split(col("Discount"), "%").getItem(0).cast("float"))
    
    
    # ---------------------- Data Integration: -------------------
    df = df1.join(df2, on="SKU", how="inner").select('SKU','Product type','Revenue generated','Number of products sold','Price','Category','Supplier Rating','Return Rate','Discount','Season','Sales Channel','Customer Segment','Store Location','Shipping Carriers','Routes','Transportation Modes','Shipping times','Shipping costs','Defect rates')


    # -------------------- Data Aggregation: ----------------------

    # Aggregate sales data to calculate total revenue, total number of products sold, and average prices by Product type and Compute average Supplier Rating and Return Rate by Category.
    agg_Product_df = df.groupBy("Product type").agg(
            sum("Revenue Generated").alias("Total_Revenue"),
            sum("Number of Products Sold").alias("Total_Products_Sold"),
            avg("Price").alias("Average_Price")
        )

    agg_cat_df = df.groupBy("Category").agg(
            avg("Supplier Rating").alias("Average_Supplier_Rating"),
            avg("Return Rate").alias("Average_Return_Rate")
        )

    agg_Product_df.show()
    agg_cat_df.show()

    agg_Product_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(output_path, 'agg_Product_df.csv'))
    agg_cat_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(output_path, 'agg_cat_df.csv'))
     


# -----------------  Identify delays and optimize shipping times.  ---------------------------------
    
    # Filter for shipments where the shipping time is greater than 5 days
    delayed_shipments = df.filter(df["Shipping times"] > 5)

    # Group by Shipping Carrier to analyze delayed shipments
    delayed_by_carrier = delayed_shipments.groupBy("Shipping Carriers").count()

    # Group by Route to see which routes are facing delays
    delayed_by_route = delayed_shipments.groupBy("Routes").count()

    # Group by Transportation Mode to identify delays by transportation method
    delayed_by_transport = delayed_shipments.groupBy("Transportation Modes").count()
    
    delayed_by_carrier.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(output_path, 'delayed_by_carrier.csv'))
    delayed_by_route.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(output_path, 'delayed_by_route.csv'))
    delayed_by_transport.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(output_path, 'delayed_by_transport.csv'))



    delayed_by_carrier.show()
    delayed_by_route.show()
    delayed_by_transport.show()


# ----------------  Choose the most cost-effective transportation modes and routes. -----------------------

    # Calculate Average Shipping Costs by Transportation Mode
    avg_shipping_cost_mode = df.groupBy("Transportation Modes").agg(
        avg("Shipping costs").alias("Average Shipping Cost")
    )

    # Calculate Average Shipping Costs by Transportation Routes
    avg_shipping_cost_route = df.groupBy("Routes").agg(
        avg("Shipping costs").alias("Average Shipping Cost")
    )

    # Calculate Average Shipping Times by Transportation Mode
    avg_shipping_time_mode = df.groupBy("Transportation Modes").agg(
        avg("Shipping times").alias("Average Shipping Time")
    )

    # Calculate Average Shipping Times by Transportation Routes.
    avg_shipping_time_route = df.groupBy("Routes").agg(
        avg("Shipping times").alias("Average Shipping Time")
    )

    # Join cost and time analysis for transportation modes
    cost_time_mode = avg_shipping_cost_mode.join(
        avg_shipping_time_mode,
        on="Transportation Modes",
        how="inner"
    )

    # Join cost and time analysis for routes
    cost_time_route = avg_shipping_cost_route.join(
        avg_shipping_time_route,
        on="Routes",
        how="inner"
    )

    # Sort modes by average cost and time to find the most cost-effective
    best_mode = cost_time_mode.sort("Average Shipping Cost").limit(1)
    best_route = cost_time_route.sort("Average Shipping Cost").limit(1)

    best_mode.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(output_path, 'best_mode.csv'))
    best_route.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(output_path, 'best_route.csv'))

    print("-------------- Best mode & route -----------")
    best_mode.show()
    best_route.show()


    
