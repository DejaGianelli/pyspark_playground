from pyspark.sql import SparkSession
import findspark

findspark.init() 

# Create SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()
    
# Create RDD from parallelize    
dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
rdd = spark.sparkContext.parallelize(dataList)

print("Initial partition count: " + str(rdd.getNumPartitions()))