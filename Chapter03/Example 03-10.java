SparkSession spark = SparkSession.builder()
						.master("local")
						.appName("SparkCSVExample")
						.config("spark.some.config.option", "some-value")
						.getOrCreate();
    	     
Dataset<Row> pricePaidDS = spark.read().option("sep","\t").csv(fileName);
