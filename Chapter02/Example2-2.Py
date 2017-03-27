dataFile = sc.textFile(“README.md”)
linesWithApache = dataFile.filter(lambda line: “Apache” in line)

