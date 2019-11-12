import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

case class node(var name : String, var equipe : String)

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


    var worgs1 = new node("Worgs Rider",  "B")
    var warlord = new node("Le Warlord", "B")
    var barbare = new node("Barbares Orc", "B")

    var solar = new node("Solar", "A")

    var graph = Array((worgs1, Array(2,3,4)), (warlord, Array(1,4)), (barbare, Array(1,4)), (solar, Array(1,2,3)))

    var rddGraph = sc.makeRDD(graph)

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
            if(edges(i).node1 == edges(j).node2 && edges(j).node1 == edges(i).node2){
                edges -= edges(j)
            }
        }
    } //Remove duplicates

    var rddEdges = sc.makeRDD(edges)

    print()

}
