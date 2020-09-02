from pyspark.sql import SparkSession


def spark_example():


    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

# df = spark.read.csv("C:\\Users\\win10\\Desktop\\VM Shared\\abc.csv", header="true")
# df.show()
# df.printSchema()
# df.select("gender").show()
# df.select(df['gender'], df['math score'] + 1).show()
# df.filter(df['math score']> 50).count() #This wont work as the datatype is not string
# df.groupBy("gender").count().show()
# df.createOrReplaceTempView("student")
# sqlDF = spark.sql("SELECT * FROM student limit 10")
# sqlDF.show()
# df.select("gender", "lunch").write.save("genderandlunch.orc", format="orc")

    df = spark.read.orc("C:\\Python Projects\\pyspark_demo\\genderandlunch.orc")
    df.show()

if __name__ == '__main__':
    spark_example()
