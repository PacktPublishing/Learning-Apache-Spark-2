JavaRDD<String> movieList = sc.parallelize(Arrays.asList("A Nous Liberte","Airplane","The Apartment","The Apartment"));
movieList.distinct().collect();