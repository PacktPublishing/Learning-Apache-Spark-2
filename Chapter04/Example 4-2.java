val transCount = transactions.cartesian(products).filter{
    case (TransProdId,ProdProdId) => TransProdId == ProdProdId
}
  .filter{case(TransProdId, ProdProdId) => ProdProdId = 3500
}
.map{
  case (TransProdId,ProdProdId) => TransProdId
}.count

Println(transCount)
