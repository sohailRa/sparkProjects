from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local').setAppName('CustomerSpent')
sc = SparkContext(conf=conf)

lines = sc.textFile('data/customer-orders.csv')

def parseLine(line):
    fields = line.split(',')
    customer = fields[0]
    amount = float(fields[2])
    return(customer, amount)


data = lines.map(lambda x: parseLine(x))
totalSpend = data.reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1])
results = totalSpend.collect()
for res in results:
    print(res) 
