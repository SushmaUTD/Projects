from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
import numpy as np 
from numpy import array
from pyspark import SparkContext
sc = SparkContext()

data = sc.textFile("/Users/sushma/Desktop/hw3datasetnew/glass.data")
pData = data.map(lambda line: array([float(x) for x in line.split(',')]))
k=[]
for i in pData.collect():
	k.append(LabeledPoint(i[-1]-1,[i[:-1]]))

lData = sc.parallelize(k)

(tData, testData) = lData.randomSplit([0.6, 0.4])
model = DecisionTree.trainClassifier(tData, numClasses=7, categoricalFeaturesInfo={},impurity='gini', maxDepth=5, maxBins=32)

pred = model.predict(testData.map(lambda x: x.features))
lAP = testData.map(lambda lp: lp.label).zip(pred)
testErr = lAP.filter(lambda (v, p): v != p).count() / float(testData.count())
acc = 1.0*lAP.filter(lambda (x, v): x == v).count() / float(testData.count())
print('Error = ' + str(testErr))
print('Accuracy {}'.format(acc))