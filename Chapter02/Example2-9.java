JavaRDD<Integer> nums = sc.parallelize(Arrays.asList( 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20));
nums.sample(true,0.1,12345).collect();
