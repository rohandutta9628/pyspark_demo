from pyspark.context import SparkContext
sc = SparkContext.getOrCreate()
raw_content = sc.textFile("2012-10-01.csv")
print(raw_content.take(5))