from pyspark.sql import SparkSession
import findspark

findspark.init() 

# Create SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()
    
data = [
    ('James', '', 'Smith', '1991-04-01', 'M', 3000),
    ('Michael', 'Rose', '', '1978-09-05', 'M', 4000)
]

schema = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]

df = spark.read.csv(encoding="UTF-8", path="inputs/addresses.csv")

df.printSchema()

df.show()