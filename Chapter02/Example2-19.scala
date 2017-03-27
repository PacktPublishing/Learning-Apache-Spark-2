#Input Data
val storeSales = sc.parallelize(Array(("London", 23.4),("Manchester",19.8),("Leeds",14.7),("London",26.6)))


#GroupByKey
storeSales.groupByKey().map(location=>(location._1,location._2.sum)).collect()

#SampleResult
#res2: Array[(String, Double)] = Array((Manchester,19.8), (London,50.0), (Leeds,14.7))

#ReduceByKey
storeSales.reduceByKey(_+_).collect()

#Sample Result
#res1: Array[(String, Double)] = Array((Manchester,19.8), (London,50.0), (Leeds,14.7))
