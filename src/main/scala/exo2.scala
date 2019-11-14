import main.session
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.script.Remove

//case class Monster(var id : Int, var name : String, var equipe : String, var armure: Int, var HP : Int)

case class edge(var Monster1 : Monster, var Monster2: Monster, var distance : Int)

object exo2 extends App {
    val conf = new SparkConf().setAppName("BDDExo2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val session = SparkSession
      .builder
      .appName("truck")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    var testAttackProche = new Sort("ntm",false,20,11)



    var worgs1 = new Monster(1,"Worgs Rider",  "B",33,20)
    var warlord = new Monster(2,"Le Warlord", "B",33,20)
    var barbare = new Monster(3,"Barbares Orc", "B",33,20)

    var solar = new Monster(4,"Solar", "A",50,50)

    var graph = Array((worgs1, Array(2,3,4)), (warlord, Array(1,4)), (barbare, Array(1,4)), (solar, Array(1,2,3,4)))

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


    var rddEdges = sc.makeRDD(edges)
    var rddGraph = sc.makeRDD(graph)




    println(rddGraph.collect()(0))
    println()

    //print result pour
    import session.implicits._
    var dfGraph = rddGraph.toDF()
    var dfEdge = rddEdges.toDF()
    dfGraph.show(200,false)
    dfEdge.show(200,false)




    solar.Attack(warlord,testAttackProche)
    rddGraph.filter(monster => monster._1.HP>0)









}
