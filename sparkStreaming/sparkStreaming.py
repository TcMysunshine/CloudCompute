from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)
words = ssc.textFileStream("/Users/chenhao/Documents/CloudCompute/data/streaming")
wordsRdd = words.map(lambda word: (word, 1))
wordCountsRdd = wordsRdd.reduceByKey(lambda x, y: x+y, 10)
# wordCountsRdd.foreachRDD(function(){
#
# })
ssc.start()
ssc.awaitTermination()