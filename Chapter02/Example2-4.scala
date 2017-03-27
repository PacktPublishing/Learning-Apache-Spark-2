val favMovies = sc.parallelize(List("Pulp Fiction","Requiem for a dream","A clockwork Orange"));
movies.flatMap(movieTitle=>movieTitle.split(" ")).collect()
