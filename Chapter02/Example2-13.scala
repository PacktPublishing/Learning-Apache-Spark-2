val java_skills=sc.parallelize(List("Tom Mahoney","Alicia Whitekar","Paul Jones","Rodney Marsh"))
val db_skills= sc.parallelize(List("James Kent","Paul Jones","Tom Mahoney","Adam Waugh"))
java_skills.intersection(db_skills).collect()
