JavaRDD<String> javaSkills= sc.parallelize(Arrays.asList("Tom Mahoney","Alicia Whitekar","Paul Jones","Rodney Marsh"));
JavaRDD<String> dbSkills= sc.parallelize(Arrays.asList("James Kent","Paul Jones","Tom Mahoney","Adam Waugh"));
javaSkills.intersection(dbSkills).collect();