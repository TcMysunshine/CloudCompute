from pyspark import SparkContext,SparkConf, SQLContext
from pyspark.streaming import StreamingContext
import os, threading, time
import logging
os.environ['PYSPARK_PYTHON'] = '/Users/chenhao/anaconda3/bin/python'


def loadVariales(filepath):
    variables = []
    with open(filepath, 'r') as r:
        lines = r.readlines()
        for line in lines:
            line = line.strip()
            variables.append(line)
    # print(variables)
    return variables


class ProvinceCount():
    def __init__(self, master,readfilepath,writefilepath):
        self.master = master
        self.readfilepath = readfilepath
        self.writefilepath = writefilepath + "MF1832013.txt"

    def provinceCount(self):
        scf = SparkConf().setAppName("ProviceCount").setMaster(self.master).set("spark.cores.max", "3")
        sc = SparkContext(conf=scf)
        # sc.setLogLevel(logging.WARNING)
        streamingContext = StreamingContext(sc, 10)
        # sqlContext = SQLContext(sc)
        province = streamingContext.textFileStream(self.readfilepath)
        # province.count()
        province_count = province.map(lambda p: (p, 1))
        province_count_rdd = province_count.reduceByKey(lambda x, y: x + y, 3)
        province_count_rdd.saveAsTextFiles(self.writefilepath)
        #一定要加这一行数据，不然不执行 WHY?
        province_count_rdd.pprint()
        province_count_rdd.foreachRDD(lambda rdd: rdd.foreachPartition(self.sendPartition))
        streamingContext.start()
        streamingContext.awaitTermination()

        # province.map(lambda p: (p, 1))
        # province.reduceByKey(lambda x, y: x + y)

    def sendPartition(self,iter):
        # ConnectionPool is a static, lazily initialized pool of connections
        with open(self.writefilepath, 'a') as a:
            for record in iter:
                print(type(record))
                a.write(record[0] + ":" + str(record[1]) + "\n")
        # return to the pool for future reuse
        # ConnectionPool.returnConnection(connection)

# dstream.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))


if __name__ == "__main__":
    master = "spark://chenhao:7077"
    variables = loadVariales("/Users/chenhao/Documents/CloudCompute/data/sparkStreaming/variables.txt")
    pc = ProvinceCount(master, variables[3], variables[4])
    pc.provinceCount()