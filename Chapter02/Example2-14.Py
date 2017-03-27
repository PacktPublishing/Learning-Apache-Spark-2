java_skills= sc.parallelize(["Tom Mahoney","Alicia Whitekar","Paul Jones","Rodney Marsh"])
db_skills= sc.parallelize(["James Kent","Paul Jones","Tom Mahoney","Adam Waugh"])
java_skills.intersection(db_skills).collect()