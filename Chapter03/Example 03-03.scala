//To read all README.md file
val dataFile = sc.textFile("README.md")

//Split line to words, and flatten the result of each split
val words = dataFile.flatMap(line => line.split(" "))
//Save to textFile
words.saveAsTextFile("/tmp/scalawords/")
