pricePaidDS = spark.write.format(“csv”).save("/home/spark/sampledata/price_paid_output”)