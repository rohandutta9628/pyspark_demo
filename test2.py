from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.read.csv("C:\\Users\\win10\\Desktop\\VM Shared\\abc.csv", header="true")
#df.show()
#df.printSchema()
#df.select("gender").show()
#df.select(df['gender'], df['math score'] + 1).show()
df.filter(df['math score']> 50).count()
df.groupBy("gender").count().show()