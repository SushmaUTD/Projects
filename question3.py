from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext
sc = SparkContext()
data = sc.textFile("/Users/sushma/Desktop/hw3datasetnew/ratings.dat")
ratings = data.map(lambda l: l.split('::')).map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))
rank = 10
numIterations = 10
(trainingData, testData) = ratings.randomSplit([0.6, 0.4])
model = ALS.train(trainingData, rank, numIterations)
testData = testData.map(lambda p: (p[0], p[1]))
predictions = model.predictAll(testData).map(lambda r: ((r[0], r[1]), r[2]))
ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
print("Mean Squared Error = " + str(MSE))

