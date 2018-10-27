from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
import os, threading, time
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


def MongoSpark(master, inputuri, outputuri, filepath):
    scf = SparkConf().setAppName("ProviceCount").setMaster(master).set("spark.cores.max", "3")
    # sparkMongo = SparkSession.builder.master(master).appName("myApp")\
    sparkMongo = SparkSession.builder.config(conf=scf) \
        .config("spark.mongodb.input.uri", inputuri) \
        .config("spark.mongodb.output.uri", outputuri) \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.3.1')\
        .getOrCreate()
    # print()
    data = sparkMongo.read.format("com.mongodb.spark.sql.DefaultSource").load()
    # data[]
    # writeData = sparkMongo.createDataFrame([('zhangwei', 24), ('zhangjiajie', 23)], ['name', 'age'])
    # writeData.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()
    # writeData.show()
    # print(data.toPandas())
    # print(data.toPandas()['head'])
    headData = data.toPandas()['head']
    provinces = []
    for head in headData:
        text = head['text'].strip()
        index = text.find("省")
        #index 方法如果不存在会抛异常
        if index > -1:
            provinces.append(text[index-2:index+1])
            # print()
    # filepath = "/Users/chenhao/Documents/CloudCompute/data/streaming"
    # print(provinces)
    t = SimuStreamThread(provinces, filepath)
    t.start()


class SimuStreamThread(threading.Thread):
    def __init__(self, data, filepath):
        #总是忘记写父类的初始化
        super(SimuStreamThread, self).__init__()
        self.data = data
        self.filepath = filepath

    def run(self):
        i = 0
        length = len(self.data)
        print("数据长度"+str(length))
        while i < length:
            start = i
            end = i + 50
            if end > length:
                data = self.data[start:length]
            else:
                data = self.data[start:end]
            timestamp = str(int(time.time()))
            filepath =self.filepath + "/head" + timestamp + ".log"
            with open(filepath, 'a') as a:
                for text in data:
                    a.write(text + "\n")
            i = end
            time.sleep(1)
            print("睡眠一秒" + str(i))


if __name__ == '__main__':
    master = "spark://chenhao:7077"
    variables = loadVariales("/Users/chenhao/Documents/CloudCompute/data/sparkStreaming/variables.txt")
    inputuri = "mongodb://"+variables[0]+"/"+variables[1]+"."+variables[2]
    outputuri = "mongodb://"+variables[0]+"/"+variables[1]+"."+variables[2]
    filepath = variables[3]
    MongoSpark(master,  inputuri, outputuri, filepath)