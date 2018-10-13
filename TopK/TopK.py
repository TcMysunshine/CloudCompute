# !/usr/bin/python
# -*- coding: UTF-8 -*-
import sys


def loadData(filePath):
    """
     通过sys模块来识别参数
    """
    # filePath = sys.argv[1]
    values = []
    with open(filePath, 'r') as r:
        lines = r.readlines()
        K = int(lines[0].strip())
        for index in range(1, len(lines)):
            value = lines[index].strip()
            values.append(value)
            # print(value)
    return K, values


def BubbleTopK(K, values):
    """测试"""
    # self.K = 7
    # self.values = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    # print(K)
    length = len(values)
    for i in range(length):
        if i < K:
            # print("第 %d 次循环" % (i + 1))
            for j in range(i + 1, length):
                # print(j)
                if values[i] < values[j]:
                    values[i], values[j] = values[j], values[i]
        else:
            break
    return values[0:K]
    # print(self.values[:-(self.K + 1):-1])


def SelectionTopK(K, values):
    results = []
    length = len(values)
    for i in range(length):
        maxIndex = i;
        if i < K:
            for j in range(i + 1, length):
                if values[maxIndex] < values[j]:
                    maxIndex = j
        else:
            break
        results.append(values[maxIndex])
    return results


def InsertTopK(K, values):
    length = len(values)
    for i in range(1, length):
        temp = values[i]
        leftIndex = i-1
        # print(leftIndex)
        while leftIndex >= 0 and temp > values[leftIndex]:
            # print(leftIndex)
            values[leftIndex+1] = values[leftIndex]
            leftIndex -= 1
        values[leftIndex+1] = temp
    return values[0:K]

def topKTest(K, values):
    values.sort(reverse=True)
    return values[0:K]


if __name__ == "__main__":
    # list = [1, 2, 3, 5, 7]
    # print(list[:-3:-1])
    filePath = '../data/TopK/evaluate.txt'
    K, values = loadData(filePath)

    testResult = topKTest(K, values)
    print("正确结果为:")
    print(testResult)

    bubbleResult = BubbleTopK(K, values)
    print("冒泡排序结果为")
    print(bubbleResult)
    if bubbleResult == testResult:
        print("冒泡排序测试通过")

    selectionResult= SelectionTopK(K, values)
    print("选择排序结果为：")
    print(selectionResult)
    if selectionResult == testResult:
        print("选择排序测试通过")
    # K = 0
    # values = [4, 2, 1, 12, 8]
    InsertResult = InsertTopK(K, values)
    print("插入排序结果为：")
    print(InsertResult)
    if InsertResult == testResult:
        print("插入排序测试通过")