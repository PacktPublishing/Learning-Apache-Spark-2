JavaRDD<String> dataFile = sc.textFile(“README.md”)
JavaRDD<String> linesWithApache = dataFile.filter(line -> line.contains(“Apache”));
