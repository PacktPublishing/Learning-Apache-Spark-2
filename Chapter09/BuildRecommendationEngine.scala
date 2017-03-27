import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql._

case class Ratings(userId: Int, movieId: Int, rating: Double, ratingTs: Long)
val ratingsSchema = Encoders.product[Ratings].schema
case class Movies(moveId: Int, title: String, genre: String)
val moviesSchema = Encoders.product[Movies].schema

val ratings = spark.read.option("header","true")
.schema(ratingsSchema)
.csv("hdfs://sparkmaster:8020/user/hdfs/sampledata/ratings.csv")

val movies = spark.read.option("header","true")
.schema(moviesSchema)
 .csv("hdfs://sparkmaster:8020/user/hdfs/sampledata/movies.csv")

 val Array(train, test) = ratings.randomSplit(Array(0.7, 0.3))
 
 val als = new ALS()
.setMaxIter(15)
.setRegParam(0.01)
.setUserCol("userId")
.setItemCol("movieId")
.setRatingCol("rating")

val recommendationModel = als.fit(train)

val predictions = recommendationModel.transform(test)
val ranks = List(1,2,3,4,5,6,7,8,9,10)
val lambdas = List(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.8,1,2,3,4,5,6,10.0)
val regParams = List(0.01,0.05,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.8,0.10,10)
val numIters = List(5,10,15,20)
var bestModel: Option[ALSModel] = None
var optimalRMSE = Double.MaxValue
var bestRank = 0
var bestRegParam = -1.0
var bestNumIter = -1

/*
 * Iterative Computation - Find best Model
 */
for (rank <- ranks; regParam <- regParams; numIter <- numIters) {
	val als = new ALS().setMaxIter(numIter).setRank(rank).setRegParam(regParam).setUserCol("userId").setItemCol("movieId").setRatingCol("rating")
	val model = als.fit(train)
	val predictions = model.transform(valid)
	val currentRMSE = evaluator.evaluate(predictions.filter("prediction <> 'NaN'"))
	println("Metrics => RMSE (Validation) = " + currentRMSE + " : Model Metrics(rank = "+ rank + ", regParam = " + regParam + ", and numIter = " + numIter + ").")
	if (currentRMSE < optimalRMSE) {
	bestModel = Some(model)
	optimalRMSE = currentRMSE
	bestRank = rank
	bestRegParam = regParam
	bestNumIter = numIter
	}
}


val als = new ALS()
.setMaxIter(15)
.setRegParam(0.01)
.setImplicitPrefs(true)
.setUserCol("userId")
.setItemCol("movieId")

