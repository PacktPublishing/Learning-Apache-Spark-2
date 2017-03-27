
//To read all README.md file
dataFile = sc.textFile("README.md")

//Split line to words, and flatten the result of each split
words = dataFile.flatMap(lambda line: line.split(" "))

//Save as TextFile
words.saveAsTextFile("/tmp/pythonwords/")
