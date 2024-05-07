import findspark

findspark.init()

# PySpark is the Spark API for Python. In this lab, we use PySpark to initialize the SparkContext.   

from pyspark import SparkContext, SparkConf

from pyspark.sql import SparkSession

# Creating a SparkContext object  

sc = SparkContext.getOrCreate()

# Creating a SparkSession  

spark = SparkSession \
    .builder \
    .appName("Python Spark DataFrames basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Download the CSV data first into a local `employees.csv` file
import wget
wget.download("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/data/employees.csv")