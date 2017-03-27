import org.apache.spark._
import org.apache.spark.streaming._

object StreamingWordCount {
	def main(args: Array[String]){
		// Create a streaming context – Donot use local[1] when running locally.
		val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
		val ssc = new StreamingContext(conf, Seconds(5))
		// Create a DStream that connects to hostname: port and fetches information at the start of streaming.
val lines = ssc.socketTextStream("localhost", 9988)
// Operate on the DStream, as you would operate on a regular stream
val words = lines.flatMap(_.split(" "))

// Count each word in each batch
val pairs = words.map(word => (word, 1))
val wordCounts = pairs.reduceByKey((x, y) => x + y)

// Print on the console
wordCounts.print()

ssc.start()             // Start the computation
ssc.awaitTermination()  // Wait for the computation to terminate
}
}
