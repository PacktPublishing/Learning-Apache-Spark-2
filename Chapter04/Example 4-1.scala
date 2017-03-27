val df = spark.read.json(“products.json”)	
case class Product(ProductId:Int, ProductName: String, ProductCategory: String)ls -l
val ds:DataSet[Product] = df.as[Product]
