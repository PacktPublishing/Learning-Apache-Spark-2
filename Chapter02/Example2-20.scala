val sampleData = sc.parallelize(Array(("k1",10),("k2",5),("k1",6),("k3",4),("k2",1),("k3",4)))
val sumCount = sampleData.combineByKey(value => (value,1), 
(valcntpair: (Int,Int), value) => (valcntpair._1 + value, valcntpair._2+1),
(valcntpair: (Int,Int), valcntpairnxt: (Int,Int)) => ((valcntpair._1 + valcntpairnxt._1),(valcntpair._2 + valcntpairnxt._2)))

sumCount.take(3)
val avgByKey = sumCount.map{case (label,value) => (label, value._1/value._2)}
avgByKey.take(3)

