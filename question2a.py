from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.util import MLUtils
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from numpy import array
sc = SparkContext()

data = sc.textFile("/Users/sushma/Desktop/hw3datasetnew/glass.data")
pdata = data.map(lambda line: array([float(x) for x in line.split(',')]))
j=[]
for i in pdata.collect():
	j.append(LabeledPoint(i[-1],[i[:-1]]))

lData = sc.parallelize(j)
t, test = lData.randomSplit([0.6, 0.4])

m = NaiveBayes.train(t, 1.0)

predictionAndLabel = test.map(lambda p: (m.predict(p.features), p.label))
acc = 1.0 * predictionAndLabel.filter(lambda (x, v): x == v).count() / test.count()
print(‘acc {}'.format(acc))
