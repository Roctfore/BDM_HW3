#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import sys
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, col, when, first, countDistinct, sum, max, expr
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

if __name__ == "__main__":
    # Get input and output paths from the command line arguments.
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    # Create a SparkSession.
    spark = SparkSession.builder.appName("BDM_HW3").getOrCreate()

    # Load the CSV file into a DataFrame and extract the required columns.
    complaints_df = spark.read.csv(input_path, header=True, sep=",")
    complaints_df = complaints_df.select("Product", "Date received", "Company")
    complaints_df = complaints_df.filter(col("Date received").rlike("^[0-9]{4}"))

    # Add a new column for the year.
    complaints_df = complaints_df.withColumn("Year", year(complaints_df["Date received"]))
    complaints_df = complaints_df.withColumn("Company",         when(col("Company").isNull(), first("Company", True).over(Window.partitionBy("Product", "Year"))).otherwise(col("Company")))

    # Replace "None" values in the Company column with a random company name.
    random_company = random.choice(complaints_df.filter(col("Company").isNotNull()).select("Company").distinct().collect())
    complaints_df = complaints_df.withColumn("Company",         when(col("Company") == "None", random_company["Company"]).otherwise(col("Company")))

    # Group by Product, Year, and Company and perform the necessary aggregations.
    grouped_df = complaints_df.groupBy("Product", "Year", "Company")                              .agg(countDistinct("Company").alias("num_categories"),                                   sum(F.lit(1)).alias("total_companies"))

    # Group by Product and Year again to merge rows with the same Product and Year.
    merged_df = grouped_df.groupBy("Product", "Year")                          .agg(sum("total_companies").alias("total_companies"),                               countDistinct("Company").alias("num_categories"))

    # Group by Product and Year to compute the maximum percentage of a single kind of Company.
    max_pct_df = merged_df.groupBy("Product", "Year")                           .agg(max(expr("1.0 * num_categories / total_companies")).alias("max_pct"))

    # Join with max_pct_df to get the maximum percentage of a single kind of Company.
    output_df = merged_df.join(max_pct_df, ["Product", "Year"])                          .select("Product", "Year", "total_companies", "num_categories", F.round(expr("max_pct*100"), 0).cast(T.IntegerType()).alias("max_pct"))                          .orderBy("Product", "Year")

    # Convert output_df to an RDD of CSV strings.
    output_rdd = output_df.rdd.map(lambda x: ",".join(str(i) for i in x))

    # Save the output to a text file.
    output_rdd.saveAsTextFile(output_path)

    # Stop the SparkSession
    spark.stop()

