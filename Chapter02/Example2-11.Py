movieList = sc.parallelize(["A Nous Liberte","Airplane","The Apartment","The Apartment"])
movieList.distinct().collect()
