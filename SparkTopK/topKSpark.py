import pyspark
from pyspark import SparkContext
from pyspark import SparkConf
import os
os.environ['PYSPARK_PYTHON']='/Users/chenhao/anaconda3/bin/python'


def loadVariales(filepath):
    variables = []
    with open(filepath, 'r') as r:
        lines = r.readlines()
        for line in lines:
            line = line.strip()
            variables.append(line)
    # print(variables)
    return variables


def topK(variables, master):
    K = int(variables[0])
    sc = SparkContext(master, 'TopK')
    data = sc.textFile("file://"+variables[1], 2)
    print(data.collect())
    id_value_rdd = data.map(lambda x: (x.split(",")[0], float(x.split(",")[1])))
    key_sum_rdd = id_value_rdd.reduceByKey(lambda x, y: x+y)
    sum_key_rdd = key_sum_rdd.sortBy(keyfunc=lambda x: x[1], ascending=False)
    with open(variables[2]+"MF1832013.txt", 'a') as w:
        #take top
        for item in sum_key_rdd.take(K):
            w.write(item[0] + "\n")


def test(master):
    # sc = SparkContext(master, 'test')
    # testRDD = sc.parallelize([('spark', (2, 3)), ('hadoop', (3, 4)),('spark', (3, 4)),('pyspark', (3, 4))])
    # # result = testRDD.mapValues(lambda x:(x,1)).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    # # result = testRDD.reduceByKey(lambda x,y:x+y)
    # result = testRDD.reduceByKey(lambda x, y: (x[0] + x[1], y[0]+y[1]))
    # print(result.collect())
    t = {}
    t['name']='chenhao'
    print(t.get('name'))
    t['name'] = 'zhan'
    print(t.get('name'))


if __name__ == '__main__':
    variables = loadVariales("../data/sparkTopK/variables.txt")
    master = "spark://chenhao:7077"
    topK(variables, master)
    # test(master)