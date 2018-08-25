from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

def ff(line):
	return(line.split()[2])

lines = sc.textFile("data/ml-100k/u.data")
ratings = lines.map(lambda x: ff(x))
result = ratings.countByValue()

sortedRes = sorted(result.items())
for key, value in sortedRes:
     print("%s %i" % (key, value))


