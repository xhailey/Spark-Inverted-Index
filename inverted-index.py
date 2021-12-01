from pyspark import SparkContext
import re

sc = SparkContext("local", "inverted index")

rdd = sc.wholeTextFiles("./Data/*/*")

def tokenizeContent(t):
    stopWords = ['they', 'she', 'he', 'it', 'the', 'as', 'is', 'and']
    file = t[0]
    content = t[1]
    cleanText = re.sub(r'[^\w\s]',' ', content.lower())
    tokens = cleanText.split()
    result = []
    for token in tokens:
        if token not in stopWords:
            result.append((file, token))
    return result

tokenizedRdd = rdd.flatMap(lambda t:tokenizeContent(t))

wordFileRdd = tokenizedRdd.map(lambda t: (t[1],[t[0]]))

wordFilesRdd = wordFileRdd.reduceByKey(lambda a,b: a+b)

def countFiles(files):
    fileCount = {}
    for file in files:
        if file in fileCount:
            fileCount[file] = fileCount[file] + 1
        else:
            fileCount[file] = 1
    return list(fileCount.items())

termFileCountRdd = wordFilesRdd.mapValues(lambda files:countFiles(files))

termFileCountRdd.saveAsTextFile("output")
