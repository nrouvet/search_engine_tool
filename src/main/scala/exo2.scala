import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

//case class sommet(var name : Double, var adjlist : Array[Int])

object exo2 extends App {
    val conf = new SparkConf().setAppName("BDDExo2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val session = SparkSession
      .builder
      .appName("")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    /*
    val nodes = sc.parallelize(Array((1L, ("Solar")), (2L, ("Pinto", )),
            (3L, ("Worgs Rider")), (4L, ("Le Warlord" ),(5L,(" Barbares Orc")))))


    val edges = sc.parallelize(Array(Edge(1L,2L),Edge(3L,4L),Edge(4L,5L),Edge(3L,5L)))
*/


}
