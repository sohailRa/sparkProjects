from pyspark import SparkContext, SparkConfig

conf = SparkConf().setAppName('Test')
sc = SparkContext(conf=conf)

lines = sc.textFile('/data/retail_db/orders/part-00000')
print(lines)
