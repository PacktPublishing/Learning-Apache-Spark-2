from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer


# Create a dataframe using labelled data set
trainingDataSet = spark.createDataFrame([
    (0, "ronaldo zidane goals score ball studs", 1.0),
    (1, "obama trump clintons whitehouse policy inflation", 0.0),
    (2, "corner penalty worldcup eurocup barcelona messie", 1.0),
    (3, "hadoop mapreduce spark goal pig hive", 0.0)], ["documentId", "corpus", "label"])

# Configure an ML pipeline, which consists of three stages:
# texttokenization, hashingTF, and logisticRegressionmodel.
textTokenizer = Tokenizer(inputCol="corpus", outputCol="words")
hashingTF = HashingTF(inputCol=textTokenizer.getOutputCol(), outputCol="features")
logisticRegressionModel = LogisticRegression(maxIter=30, regParam=0.01)
pipeline = Pipeline(stages=[textTokenizer, hashingTF, logisticRegressionModel])

# Fit the pipeline to training documents.
#Returns a model which can then be used with other data sets for prediction.

model = pipeline.fit(trainingDataSet)

# Create a dataset which contains unlabelled documents of data
testDataSet = spark.createDataFrame([
    (4, "corner ball goal score" ),
    (5, "sort hive optimzer columnar"),
    (6, "ronaldo messie eurocup"),
    (7, "database parquet orc avro")], ["documentId", "corpus"])

# Make predictions on test documents and print columns of interest from the predictions.
prediction = model.transform(testDataSet)
selectedColumns = prediction.select("documentId", "corpus", "prediction", "probability")
for eachRow in selectedColumns.collect():
    print(eachRow)
