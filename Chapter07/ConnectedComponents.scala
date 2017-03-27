import org.apache.spark.graphx._
val cdrGraph = GraphLoader.edgeListFile(sc,"/home/spark/sampledata/graphx/cdrs.txt")
val connectedVertices = cdrGraph.connectedComponents().vertices
val usersList = sc.textFile("/home/spark/sampledata/graphx/usernames.csv").map{line =>
val fields = line.split(",")
(fields(0).trim().toLong, fields(1))
}
val connectedComponentsByUsers = usersList.join(connectedVertices).map {
  case (id, (username, cc)) => (username, cc)
}
println(connectedComponentsByUsers.collect().mkString("\n"))
