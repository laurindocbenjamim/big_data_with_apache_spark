import findspark # type: ignore
import wget

findspark.init()

# PySpark is the Spark API for Python. In this lab, we use PySpark to initialize the SparkContext.   

from pyspark import SparkContext, SparkConf

from pyspark.sql import SparkSession


# Creating a SparkContext object

sc = SparkContext.getOrCreate()

# Creating a Spark Session

spark = SparkSession \
    .builder \
    .appName("Python Spark DataFrames basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Load data from host  using wget library
link_to_data_1 = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/dataset1.csv"
#wget.download(link_to_data_1)
link_to_data_2 = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/dataset2.csv"
#wget.download(link_to_data_2, "files/dataset_2.csv")

# Load the d ata int a pyspark dataframe
print("Load teh data into a pyspark dataframe")
df1 = spark.read.csv("dataset1.csv", header=True, inferSchema=True)
df2 = spark.read.csv("files/dataset_2.csv", header=True, inferSchema=True)

# Display the schema  of dataframe
print("Display the schema of dataframe")
df1.printSchema()
df2.printSchema()

from pyspark.sql.functions import year, quarter, to_date
#Add a new column to each dataframe
df1 = df1.withColumn('year', year(to_date('date_column', 'dd/MM/yyyy')))
df2 = df2.withColumn('quarter', quarter(to_date('transaction_date','dd/MM/yyyy')))

#Rename df1 column amount to transaction_amount

df1 = df1.withColumnRenamed('amount', 'transaction_amount')
#Rename df2 column value to transaction_value
df2 = df2.withColumnRenamed('value', 'transaction_value')

#Drop columns description and location from df1
df1 = df1.drop('description', 'location')
    
#Drop column notes from df2
df2 = df2.drop('notes')

#join df1 and df2 based on common column customer_id
joined_df = df1.join(df2, 'customer_id', 'inner')

# filter the dataframe for transaction amount > 1000
filtered_df =  joined_df.filter("transaction_amount > 1000")

# Agregating data by customer

from pyspark.sql.functions import sum
# group by customer_id and aggregate the sum of transaction amount
total_amount_per_customer = filtered_df.groupBy('customer_id').agg(sum('transaction_amount').alias('total_amount'))

#display the result
total_amount_per_customer.show()

#Write the result to a Hive table

# Write total_amount_per_customer to a Hive table named customer_totals
total_amount_per_customer.write.mode("overwrite").saveAsTable("customer_totals")

#Write filtered_df to HDFS in parquet format file filtered_data.parquet
filtered_df.write.mode("overwrite").parquet("filtered_data.parquet")

# Add new column with value indicating whether transaction amount is > 5000 or not
from pyspark.sql.functions import when, lit

df1 = df1.withColumn("high_value", when(df1.transaction_amount > 5000, lit("Yes")).otherwise(lit("No")))

#calculate the average transaction value for each quarter in df2
from pyspark.sql.functions import avg

average_value_per_quarter = df2.groupBy('quarter').agg(avg("transaction_value").alias("avg_trans_val"))
#show the average transaction value for each quarter in df2    
average_value_per_quarter.show()

#Write average_value_per_quarter to a Hive table named quarterly_averages
average_value_per_quarter.write.mode("overwrite").saveAsTable("quarterly_averages")

# calculate the total transaction value for each year in df1.
total_value_per_year = df1.groupBy('year').agg(sum("transaction_amount").alias("total_transaction_val"))

# show the total transaction value for each year in df1.
total_value_per_year.show()

#Write total_value_per_year to HDFS in the CSV format
total_value_per_year.write.mode("overwrite").csv("total_value_per_year.csv")
