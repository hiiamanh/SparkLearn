from pyspark import SparkConf, SparkContext
import collections

# We are going to be running on a local box only
# not on a cluster
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

#Load data
#It breaks up that input file line by line, so that every line of text correcsponds
#to one value in your RDD
lines = sc.textFile("D:/LearningSpark/ml-100k/u.data")

ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
