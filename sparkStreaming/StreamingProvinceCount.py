from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession, Row
from pyspark.streaming import StreamingContext
import os, threading, time
import pandas as pd

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

# globals()['province_count'] = []


class ProvinceCount():
    def __init__(self, master, readfilepath, writefilepath):
        self.master = master
        self.readfilepath = readfilepath
        self.writefilepath = writefilepath + "MF1832013.txt"
        self.province_count_df = None
        # # sc.setLogLevel(logging.WARNING)
        # self.streamingContext = StreamingContext(self.sc, 10)


    def getSparkSessionInstance(self, sparkConf):
        if ('sparkSessionSingletonInstance' not in globals()):
            globals()['sparkSessionSingletonInstance'] = SparkSession \
                .builder \
                .config(conf=sparkConf) \
                .getOrCreate()
        return globals()['sparkSessionSingletonInstance']

    # checkpoint检查点机制，如果程序挂掉重启时会重新从断掉的时间开始监听
    def provinceCount(self):
        scf = SparkConf().setAppName("ProviceCount").setMaster(self.master).set("spark.cores.max", "3")
        sc = SparkContext(conf=scf)
        # sc.setLogLevel(logging.WARNING)
        streamingContext = StreamingContext(sc, 10)
        sparkSession = SparkSession.builder.config(conf=scf).getOrCreate()
        # sparkSession = SparkSession.builder.config(conf=scf).getOrCreate()
        # remember函数
        # streamingContext.checkpoint("file:///Users/chenhao/Documents/CloudCompute/data/checkpoint")
        province = streamingContext.textFileStream(self.readfilepath)
        # province.saveAsTextFiles()
        # streamingContext.
        #hubei,1
        province_count = province.map(lambda p: (p, 1))
        # 每一次读取的数据
        province_count_rdd = province_count.reduceByKey(lambda x, y: x + y)
        # province_count_rdd.s
        # 目前为止所有的数据
        # province_count_update_rdd = province_count.updateStateByKey(self.provinceCountAdd) #这行代码需要加上checkpoint why?
        # province_count_rdd.saveAsTextFiles(self.writefilepath)
        # province_count_update_rdd.checkpoint(8*1000*10)
        # province_count_update_rdd.foreachRDD(lambda rdd: rdd.foreachPartition(self.sendPartition))
        province_count_rdd.foreachRDD(self.process)
        # 一定要加这一行数据，不然不执行 WHY?
        province_count_rdd.pprint(5)
        # province_count_update_rdd.pprint(5)
        # print("主函数" + self.province_count_df)
        streamingContext.start()
        streamingContext.awaitTermination(timeout=1000)

    def process(self, time, rdd):
        print("时间" + str(time))
        if not rdd.isEmpty():
            print("数据处理")
            try:
                spark = self.getSparkSessionInstance(rdd.context.getConf())
                rowRDD = rdd.map(lambda x: Row(province=x[0], count=x[1]))
                temp_province_count_df = spark.createDataFrame(rowRDD)
                temp_province_count_df.show()
                if self.province_count_df is not None:
                    self.province_count_df = pd.concat([self.province_count_df, temp_province_count_df.toPandas()], sort=True, axis=0)
                    print(self.province_count_df.shape)
                else:
                    self.province_count_df = temp_province_count_df.toPandas()
                    # self.province_count_df.show()
            except BaseException as e:
                print(e)
        if self.province_count_df is not None:
            self.writefile()

    def writefile(self):
        #resetINdex
        self.province_count_df = self.province_count_df.groupby('province').sum().reset_index()
        row = self.province_count_df.shape[0]
        with open(self.writefilepath, 'w') as w:
            for i in range(row):
                province_count_data = self.province_count_df.iloc[i]
                print(province_count_data['province'] + ":" + str(province_count_data['count']))
                w.write(province_count_data['province'] + ":" + str(province_count_data['count'])+"\n")

    def provinceCountAdd(self, newValues, runningCount):
        if runningCount is None:
            runningCount = 0
        return sum(newValues, runningCount)

    def sendPartition(self, item):
        with open(self.writefilepath, 'a') as a:
            for record in item:
                a.write(record[0] + ":" + str(record[1]) + "\n")


# dstream.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))


if __name__ == "__main__":
    master = "local[*]"
    variables = loadVariales("/Users/chenhao/Documents/CloudCompute/data/sparkStreaming/variables.txt")
    pc = ProvinceCount(master, variables[3], variables[4])
    pc.provinceCount()
