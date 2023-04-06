#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import year, countDistinct, sum, max, expr, when, first, col
from pyspark.sql.window import Window
import sys

if __name__ == "__main__":
    # Check the number of arguments passed in
    if len(sys.argv) != 3:
        print("Usage: BDM_HW3_yl9908.py <input_file> <output_dir>", file=sys.stderr)
        sys.exit(-1)

    # Set the input and output paths
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    # Create a SparkSession
    spark = SparkSession.builder.appName("BDM_HW3_yl9908").getOrCreate()

    # Load the CSV file into a DataFrame and extract the required columns.
    complaints_df = spark.read.csv(input_path, header=True, sep=",")
    complaints_df = complaints_df.select("Product", "Date received", "Company")

    # Filter out invalid Date received values
    complaints_df = complaints_df.filter(col("Date received").rlike("^[0-9]{4}"))

    # Add a new column for the year.
    complaints_df = complaints_df.withColumn("Year", year(complaints_df["Date received"]))

    # Fill null values in the Company column with a random company name.
    complaints_df = complaints_df.withColumn("Company", when(col("Company").isNull(), first("Company", True).over(Window.partitionBy("Product", "Year"))).otherwise(col("Company")))

    # Group by Product, Year, and Company and perform the necessary aggregations.
    grouped_df = complaints_df.groupBy("Product", "Year", "Company")                              .agg(countDistinct("Company").alias("num_categories"),                                   sum(expr("CASE WHEN Company IS NOT NULL THEN 1 ELSE 0 END")).alias("total_companies"))

    # Group by Product and Year to compute the maximum percentage of a single kind of Company.
    max_pct_df = grouped_df.groupBy("Product", "Year")                           .agg(max(expr("num_categories/total_companies")).alias("max_pct"))

    # Group by Product and Year again to merge rows with the same Product and Year.
    merged_df = grouped_df.groupBy("Product", "Year")                          .agg(sum("total_companies").alias("total_companies"),                               countDistinct("Company").alias("num_categories"))

    # Join with max_pct_df to get the maximum percentage of a single kind of Company.
    output_df = merged_df.join(max_pct_df, ["Product", "Year"])                          .select("Product", "Year", "total_companies", "num_categories", expr("ROUND(max_pct, 0)").cast("int").alias("max_pct"))                          .orderBy("Product", "Year")

    # Convert output_df to an RDD of CSV strings.
    output_rdd = output_df.rdd.map(lambda x: ",".join(str(i) for i in x))

    # Save the output to a text file.
    output_rdd.saveAsTextFile(output_path)

    # Stop the SparkSession
    spark.stop()

