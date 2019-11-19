import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

//case class node(var name : String, var equipe : String)
//case class Monster(var id : Int, var name : String, var equipe : String, var armure: Int, var HP : Int)

case class edge(var monster1 : Monster, var monster2: Monster, var distance : Int)

object exo2 extends App {
    val conf = new SparkConf().setAppName("BDDExo2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val session = SparkSession
      .builder
      .appName("")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    var worgs1 = new Monster("Worgs Rider",  "B", 44, 100)
    var warlord = new Monster("Le Warlord", "B", 44, 100)
    var barbare = new Monster("Barbares Orc", "B", 44, 100)

    var solar = new Monster("Solar", "A", 150, 1000)

    var graph = Array((worgs1, Array(2,3,4)), (warlord, Array(1,4)), (barbare, Array(1,4)), (solar, Array(1,2,3)))



    var edges = ArrayBuffer.empty[edge]

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
            if(edges(i).monster1 == edges(j).monster2 && edges(j).monster1 == edges(i).monster2){
                edges -= edges(j)
            }
        }
    } //Remove duplicates

    var rddGraph = sc.makeRDD(graph)
    var rddEdges = sc.makeRDD(edges)

    print(rddGraph.collect().foreach(println))

    /*
    val nodes = sc.parallelize(Array((1L, ("Solar")), (2L, ("Pinto", )),
            (3L, ("Worgs Rider")), (4L, ("Le Warlord" ),(5L,(" Barbares Orc")))))


    val edges = sc.parallelize(Array(Edge(1L,2L),Edge(3L,4L),Edge(4L,5L),Edge(3L,5L)))
*/


}
