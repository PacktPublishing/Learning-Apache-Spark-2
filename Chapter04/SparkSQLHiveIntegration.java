# Create Spark Session with Hive Support
	SparkSession mySparkSession = SparkSession.builder()
   				.master("local")
   				.appName("Java Spark-SQL Hive Integration ")
   				.enableHiveSupport()
   				.config("spark.sql.warehouse.dir", sparkWarehouseDir)
   				.getOrCreate();
   
	# Create Table
 	mySparkSession.sql("CREATE TABLE IF NOT EXISTS "
+" CDRs (callingNumber STRING, calledNumber String, “   
+” origin String, Dest String,CallDtTm String, callCharge Int) "
		+" ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
 	
	# Load CDRs data    	
   	mySparkSession.sql("LOAD DATA LOCAL INPATH '"+fileName+"' “
+”INTO TABLE CDRs");
    	
	#Manipulating the data using Hive-QL   
	mySparkSession.sql(" SELECT origin, dest, count(*) as cnt "
    					  +" FROM CDRs "
    					  +" GROUP by origin, dest "
    					  +" ORDERR by cnt desc "
    					  +"  LIMIT 5").show();
