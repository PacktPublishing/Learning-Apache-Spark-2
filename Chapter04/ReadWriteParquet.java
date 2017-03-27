#Loading a JSON file as a DataSet of Row objects
Dataset<Row> callDetailsDF = mySparkSession.read().json(fileName);

#Writing a Parquet File
callDetailsDF.write().parquet(parquetFileName);

#Reading a Parquet file of Dataset of Row objects
Dataset<Row> callDetailsParquetDF = mySparkSession.read().parquet(parquetFileName);

#Parquet file data manipulation
callDetailsParquetDF.createOrReplaceTempView("callDetails");
Dataset<Row> topLocDF = mySparkSession.sql("select Origin,Dest, count(*) as cnt from calldetails group by Origin,Dest  order by cnt desc");
topLocDF.show(5);
