from pyspark.sql import SparkSession
import findspark

findspark.init() 

# Create SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()
    
# Create RDD from external Data Source
rdd2 = spark.sparkContext.textFile("inputs/test.txt")