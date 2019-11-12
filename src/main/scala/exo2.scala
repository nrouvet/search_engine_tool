import main.session
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

case class node(var name : String, var equipe : String)
//case class Monster(var id : Int, var name : String, var equipe : String, var armure: Int, var HP : Int)

case class edge(var node1 : node, var node2: node, var distance : Int)

object exo2 extends App {
    val conf = new SparkConf().setAppName("BDDExo2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val session = SparkSession
      .builder
      .appName("truck")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


<<<<<<< Updated upstream
    var worgs1 = new node("Worgs Rider",  "B")
    var warlord = new node("Le Warlord", "B")
    var barbare = new node("Barbares Orc", "B")

    var solar = new node("Solar", "A")
=======
    var worgs1 = new Monster(1,"Worgs Rider",  "B",33,20)
    var warlord = new Monster(2,"Le Warlord", "B",33,20)
    var barbare = new Monster(3,"Barbares Orc", "B",33,20)

    var solar = new Monster(4,"Solar", "A",50,50)
>>>>>>> Stashed changes

    var graph = Array((worgs1, Array(2,3,4)), (warlord, Array(1,4)), (barbare, Array(1,4)), (solar, Array(1,2,3)))

<<<<<<< Updated upstream
    var rddGraph = sc.makeRDD(graph)

    var edges = ArrayBuffer.empty[edge]
=======
    var edges = Array.empty[edge]
>>>>>>> Stashed changes

    for(i <- 0 until graph.length){
        for(j<-0 until graph(i)._2.length){
            if (graph(graph(i)._2(j)-1)._1.equipe == graph(i)._1.equipe) {
                edges += edge(graph(i)._1, graph(graph(i)._2(j) - 1)._1, 50)
            }
            else {
                edges += edge(graph(i)._1, graph(graph(i)._2(j)-1)._1, 110)
            }
        }
    }

    for(i<-0 until edges.length) {
        for(j<-i+1 until edges.length-1) {
            if(edges(i).node1 == edges(j).node2 && edges(j).node1 == edges(i).node2){
                edges -= edges(j)
            }
        }
    } //Remove duplicates

    var rddEdges = sc.makeRDD(edges)
    var rddGraph = sc.makeRDD(graph)

<<<<<<< Updated upstream
    print()

    /*
    val nodes = sc.parallelize(Array((1L, ("Solar")), (2L, ("Pinto", )),
            (3L, ("Worgs Rider")), (4L, ("Le Warlord" ),(5L,(" Barbares Orc")))))

=======
    print(rddGraph.collect()(1))
    println()
>>>>>>> Stashed changes

    val edges = sc.parallelize(Array(Edge(1L,2L),Edge(3L,4L),Edge(4L,5L),Edge(3L,5L)))
*/


}
