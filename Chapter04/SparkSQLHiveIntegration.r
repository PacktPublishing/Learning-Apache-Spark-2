# Creating Spark Session with hive Support
sparkR.session(enableHiveSupport=TRUE)

# Creating a table to hold CDRs
sql("CREATE TABLE IF NOT EXISTS cdrs(callingNumber STRING, calledNumber String, origin String, Dest String,CallDtTm String, callCharge Int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")

# Loading data
sql("LOAD DATA LOCAL INPATH '/home/spark/sampledata/cdrs.csv' INTO table cdrs")

# Finding top paired origin/destinations
sql(" SELECT origin, dest, count(*) as cnt from cdrs group by origin, dest order by cnt desc LIMIT 5")
