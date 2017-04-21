from numpy import array
from collections import defaultdict
from pyspark.mllib.clustering import KMeans
from pyspark import SparkContext
sc = SparkContext()

datafile = sc.textFile("/Users/sushma/Desktop/hw3datasetnew/itemusermat")
data = datafile.map(lambda line: array([float(x) for x in line.split(' ')]))
cluster = KMeans.train(data, 10, maxIterations=10, initializationMode="random")
x=cluster.predict(data)
l=list(enumerate(x.collect()))
clusterlist = defaultdict(list)
for i in l:
	clusterlist[i[1]].append(i[0])
moviedata = sc.textFile("/Users/sushma/Desktop/hw3datasetnew/movies.dat")
movieData = moviedata.map(lambda line: array([x for x in line.split('::')]))
movielist = {}
for i in movieData.collect():
	movielist[int(i[0])] = i[1:]
count = 0
for i in clusterlist:
	print "cluster :", i+1
	for j in clusterlist[1]:
		print movielist[j][0]+" "+movielist[j][1]
		count=count+1
		if(count>5):
			break
	count=0