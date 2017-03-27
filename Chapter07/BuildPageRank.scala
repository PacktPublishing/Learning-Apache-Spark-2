import org.apache.spark.graphx.{Graph, VertexRDD, GraphLoader}
val cdrGraph = GraphLoader.edgeListFile(sc,"/home/spark/sampledata/graphx/cdrs.txt")
val influencers = cdrGraph.pageRank(0.0001).vertices
val usersList = sc.textFile("/home/spark/sampledata/graphx/usernames.csv").map{line =>
val fields = line.split(",")
(fields(0).trim().toLong, fields(1))
}

val ranksByUsername = usersList.join(influencers).map {
  case (id, (username, userRank)) => (username, userRank)
}
println(ranksByUsername.collect().mkString("\n"))

