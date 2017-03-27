#Loading a JSON file as a DataFrame
callDetailsDF <- read.df("/home/spark/sampledata/json/cdrs.json","json")
#Writing the DataFrame out as a Parquet
write.parquet(callDetailsDF,"cdrs.parquet")
#Reading Parquet as a DataFrame
callDetailsParquetDF <- read.parquet("cdrs.parquet")
#Data Manipulation of Parquet Data
createOrReplaceTempView(callDetailsParquetDF,"parquetFile")
topCallLocsDF <- sql("select Origin,Dest, count(*) as cnt from calldetails group by Origin,Dest  order by cnt desc")
head(topCallLocsDF)
