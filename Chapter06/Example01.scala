import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector,Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

val textTokenizer = new Tokenizer()
  .setInputCol("corpus")
  .setOutputCol("tokenizedWords")
/* HashingTF and CountVectorized can be used to generate term frequencies. HashingTF utilizes that hashing trick and is a very fast and space-efficient way of turning arbitrary features into a vector or a matrix.
*/
 
val hashingTermFrequency = new HashingTF()
  .setNumFeatures(1000)
  .setInputCol(tokenizer.getOutputCol)
  .setOutputCol("features")
val logisticRegression = new LogisticRegression()
  .setMaxIter(10)
  .setRegParam(0.01)
val pipeline = new Pipeline()
  .setStages(Array(tokenizer, hashingTermFrequency, logisticRegression))
val model = pipeline.fit(trainingDataset)
