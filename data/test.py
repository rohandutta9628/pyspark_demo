#df = sc.read.format('csv').options(header='true').load('C:\\Users\\win10\\Desktop\\VM Shared\\abc.csv')
from pyspark import SparkContext
from pyspark.sql import *
sc = SparkContext("local", "count app")
words = sc.parallelize (
   ["scala",
   "java",
   "hadoop",
   "spark",
   "akka",
   "spark vs hadoop",
   "pyspark",
   "pyspark and spark"]
)
counts = words.count()
print("Number of elements in RDD -> %i" % (counts))

coll = words.collect()
print("Elements in RDD -> %s" % (coll))

def f(x): print(x+" Hello")
fore = words.foreach(f)

words_filter = words.filter(lambda x: 'spark' in x)
filtered = words_filter.collect()
print("Fitered RDD -> %s" % (filtered))
