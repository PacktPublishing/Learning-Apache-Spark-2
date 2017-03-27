//To read all README.md file
  JavaRDD<String> dataFile = sc.textFile(fileName);
  
//Split line to words, and flatten the result of each split
JavaRDD<String> words = dataFile.flatMap(line ->  Arrays.asList(line.split(" ")).iterator());
  
//Save as TextFile
words.saveAsTextFile(outputFile);
