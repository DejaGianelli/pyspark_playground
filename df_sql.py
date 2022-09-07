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
    ('Michael', 'Rose', '', '1978-09-05', 'M', 4000),
    ('Karen', 'Perry', '', '1988-03-05', 'F', 7000)
]

schema = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]

df = spark.createDataFrame(data=data, schema=schema)

df.createOrReplaceTempView("customer")
df2 = spark.sql("SELECT * FROM customer")

df2.printSchema()
df2.show()

df3 = spark.sql("SELECT gender, count(*) AS gender_count FROM customer GROUP BY gender")
df3.show()