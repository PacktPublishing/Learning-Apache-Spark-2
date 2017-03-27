movies = sc.parallelize(["Pulp Fiction","Requiem for a dream","A clockwork Orange"])
movies.flatMap(lambda movieTitle: movieTitle.split(" ")).collect()
