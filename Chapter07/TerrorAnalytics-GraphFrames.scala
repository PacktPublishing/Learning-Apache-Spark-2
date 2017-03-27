import org.graphframes.GraphFrame
//Creating a Vertices Data Frame – Remember to specify the “id” column
val vertices = spark.createDataFrame(List(
("Maggie","UK",28,"Female"),
("Jennifer","US",42,"Female"),
("Roger","US",42,"Male"),
("Ben","US",30,"Male"),
("Tom","UK",41,"Male"),
("Terrorism","N/A",0,"N/A"),
("Hate-Speech","N/A",0,"N/A"),
("Sports","N/A",0,"N/A"),
("Politics","N/A",0,"N/A")
)).toDF("id","Country","Age","Gender")

//Creating an Edges Data Frame – Remember to specify the “src” and “dst” //columns
 val edges = spark.createDataFrame(List(
 ("Maggie","Terrorism","Creates"),
 ("Ben","Terrorism","Likes"),
 ("Maggie","Hate-Speech","Creates"),
 ("Jennifer","Terrorism","Likes"),
 ("Maggie","Terrorism","Creates"),
 ("Ben","Hate-Speech","Shares"),
 ("Jennifer","Sports","Creates"),
 ("Roger","Sports","Likes"),
 ("Roger","Politics","Likes"),
 ("Tom","Sports","Likes"),
 ("Tom","Politics","Creates")
  )).toDF("src","dst","relationship")

  // Creating the Graph Frame by passing the vertices and edges data frames to the GraphFrame class constructor.
  val terrorAnalytics = GraphFrame(vertices,edges)

//You can run degrees on this GraphFrame as follows
terrorAnalytics.degrees.show()
