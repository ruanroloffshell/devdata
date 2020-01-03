import sys
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":

	spark = SparkSession\
		.builder\
		.appName("PythonWordCount")\
		.getOrCreate()

	sc = spark.sparkContext

	words = "he quick brown fox jumps over the lazy dog the quick brown fox jumps over the lazy dog"
	lines = sc.parallelize(words)
	counts = lines.flatMap(lambda x: x.split(' ')) \
		.map(lambda x: (x, 1)) \
		.reduceByKey(add)
				  
	output = counts.collect()
	for (word, count) in output:
		print("%s: %i" % (word, count))

	spark.stop()