val movieList = sc.parallelize(List("A Nous Liberte","Airplane","The Apartment","The Apartment"))
moviesList.distinct().collect()
