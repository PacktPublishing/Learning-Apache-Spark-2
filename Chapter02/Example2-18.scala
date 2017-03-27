sc.parallelize(Seq(10, 4, 2, 12, 3)).takeOrdered(1)
// returns Array(2)

sc.parallelize(Seq(2, 3, 4, 5, 6)).takeOrdered(2)
// returns Array(2, 3)
