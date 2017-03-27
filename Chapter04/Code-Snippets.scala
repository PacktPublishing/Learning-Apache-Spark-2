//Code Snippet 1
val df = spark.read.json(“products.json”)	
case class Product(ProductId:Int, ProductName: String, ProductCategory: String)ls -l
val ds:DataSet[Product] = df.as[Product]

//Code Snippet 2
val transCount = transactions.cartesian(products).filter{
    case (TransProdId,ProdProdId) => TransProdId == ProdProdId
}
  .filter{case(TransProdId, ProdProdId) => ProdProdId = 3500
}
.map{
  case (TransProdId,ProdProdId) => TransProdId
}.count

Println(transCount)


//Code Snippet 3
val filteredProducts =  products.filter(prodProdId = 3500)
val preparedTransactions = Transactions.map(TransProdId => (TransProdId, TransProdId))
val preparedProducts = filteredProducts.map(prodProdId => (prodProdId, prodProdId))

val transCount = preparedTransactions.join(preparedProducts).map {
		case (transProdId, _) => transProdId}.count

Println(transCount)
