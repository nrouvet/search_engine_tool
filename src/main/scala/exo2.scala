import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

case class node(var id : Int, var name : String, var equipe : String)

case class edge(var node1 : node, var node2: node, var distance : Int)

object exo2 extends App {
    val conf = new SparkConf().setAppName("BDDExo2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val session = SparkSession
      .builder
      .appName("")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    var worgs1 = new node(1,"Worgs Rider",  "B")
    var warlord = new node(2,"Le Warlord", "B")
    var barbare = new node(3,"Barbares Orc", "B")

    var solar = new node(4,"Solar", "A")

    var graph = Array((worgs1, Array(2,3,4)), (warlord, Array(1,4)), (barbare, Array(1,4)), (solar, Array(1,2,3,4)))

    var rddGraph = sc.makeRDD(graph)

    var edges = Array.empty[edge]

    for(i <- 0 until graph.length){
        for(j<-0 until graph(i)._2.length){
            if (graph(graph(i)._2(j)-1)._1.equipe == graph(i)._1.equipe) {
                var tmp = new edge(graph(i)._1, graph(graph(i)._2(j)-1)._1, 50)
                edges = edges :+ tmp
            }
            else {
                var tmp = new edge(graph(i)._1, graph(graph(i)._2(j)-1)._1, 110)
                edges = edges :+ tmp
            }
        }
    }

/*
    var rddEdges = rddGraph.map{
        monster =>
            monster._2.map {
              value =>
                      if (graph(value-1)._1.equipe == monster._1.equipe) {
                          var tmp = new edge(monster._1, graph(value-1)._1, 50)
                          tmp
                      }
                      else {
                          var tmp = new edge(monster._1, graph(value-1)._1, 110)
                          tmp
                      }
          }
    }
*/
    var rddEdges = sc.makeRDD(edges)

    print()

    /*
    val nodes = sc.parallelize(Array((1L, ("Solar")), (2L, ("Pinto", )),
            (3L, ("Worgs Rider")), (4L, ("Le Warlord" ),(5L,(" Barbares Orc")))))


    val edges = sc.parallelize(Array(Edge(1L,2L),Edge(3L,4L),Edge(4L,5L),Edge(3L,5L)))
*/


}
