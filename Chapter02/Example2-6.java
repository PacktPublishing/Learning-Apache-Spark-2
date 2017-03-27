JavaRDD<String> movies = sc.parallelize
(Arrays.asList("Pulp Fiction","Requiem for a dream"
,"A clockwork Orange")
);
    	
JavaRDD<String> movieName = movies.flatMap(
        new FlatMapFunction<String,String>(){
          public Iterator<String> call(String movie){
            return Arrays.asList(movie.split(" "))
 .iterator();
           }
      }
);
