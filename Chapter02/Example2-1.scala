val dataFile = sc.textFile(“README.md”)
val linesWithApache = dataFile.filter(line => line.contains(“Apache”))
