#Reading a JSON file as a DataFrame
callDetailsDF = spark.read.json("/home/spark/sampledata/json/cdrs.json")
# Write the DataFrame out as a Parquet File
callDetailsDF.write.parquet("cdrs.parquet")
# Loading the Parquet File as a DataFrame
callDetailsParquetDF = spark.read.parquet("cdrs.parquet")
# Standard DataFrame data manipulation
callDetailsParquetDF.createOrReplaceTempView("calldetails")
topCallLocsDF = spark.sql("select Origin,Dest, count(*) as cnt from calldetails group by Origin,Dest  order by cnt desc")
