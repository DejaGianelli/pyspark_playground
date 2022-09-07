from pyspark.sql import SparkSession
import findspark

findspark.init() 

# Create SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()
    
