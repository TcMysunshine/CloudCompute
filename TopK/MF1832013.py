import sys


def loadData():
    """
     通过sys模块来识别参数
    """
    filePath = sys.argv[1]
    values = []
    with open(filePath, 'r') as r:
        lines = r.readlines()
        K = int(lines[0].strip())
        for index in range(1, len(lines)):
            value = lines[index].strip()
            values.append(value)
            # print(value)
    return K, values


def InsertTopK(K, values):
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


if __name__ == "__main__":
    K, values = loadData()
    InsertResult = InsertTopK(K, values)
    print(",".join(InsertResult))