# Creating a Spark session with hive Support
customSparkSession = SparkSession.builder \c
appName("Ptyhon Sparl SQL and Hive Integration ") \
config("spark.sql.warehouse.dir","spark-warehouse") \
enableHiveSupport() \
getOrCreate()
# Creating a Table
customSparkSession.sql(“CREATE TABLE IF NOT EXISTS cdrs
(callingNumber STRING, calledNumber String, origin String, Dest String,CallDtTm String, callCharge Int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ”)
# Loading Data into Hive CDRs table
customSparkSession. sql("LOAD DATA LOCAL INPATH '/home/spark/sampledata/cdrs.csv' INTO table cdrs")
# Viewing the Loaded data
customSparkSession.sql("SELECT * from cdrs LIMIT 5").show()
# Viewing top 5 Origin destination Pairs
customSparkSession. sql(" SELECT origin, dest, count(*) as cnt from cdrs group by origin, dest order by cnt desc LIMIT 5").show()
