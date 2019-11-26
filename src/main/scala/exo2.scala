import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random
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

    var testAttackProche = Sort("ntm",false,List(35,30,25,20),50, 3, 6)
    var testAttackLoin = Sort("fdp",false,List(30, 25, 20),120, 2, 6)

    var listSort = List(testAttackLoin,testAttackProche)

    println(listSort)


    var worgs1 = Monster(1,"Worgs Rider",  "B",33,20,List(null),  1)
    var warlord = Monster(2,"Le Warlord", "B",33,20,List(null),  1)
    var barbare = Monster(3,"Barbares Orc", "B",33,20,List(null), 1)

    var solar = Monster(4,"Solar", "A",50,50,List(testAttackProche,testAttackLoin), 4)

    var graph = Array((worgs1, Array(2,3,4)), (warlord, Array(1,4)), (barbare, Array(1,4)), (solar, Array(1,2,3,4)))

    var edges = Array.empty[edge]

    for(i <- 0 until graph.length){
        for(j<-0 until graph(i)._2.length){
            if (graph(graph(i)._2(j)-1)._1.equipe == graph(i)._1.equipe) {
                var tmp = edge(graph(i)._1, graph(graph(i)._2(j)-1)._1, 50)
                edges = edges :+ tmp
            }
            else {
                var tmp = edge(graph(i)._1, graph(graph(i)._2(j)-1)._1, 110)
                edges = edges :+ tmp
            }
        }
    }


    var rddEdges = sc.makeRDD(edges).cache()
    var rddGraph = sc.makeRDD(graph).cache()




    println(rddGraph.collect()(0))
    println()

    //print result pour test attack
    import session.implicits._
    var dfGraph = rddGraph.toDF()
    var dfEdge = rddEdges.toDF()
    dfGraph.show(200,false)
    dfEdge.show(200,false)
    solar.Attack(warlord,testAttackProche)//OK
    rddGraph.filter(monster => monster._1.HP>0)


    dfGraph.printSchema()
    val process = dfEdge.map( row=> (row.getInt(2)))
    process.show()



    //OK retourne la distance entre deux monstre
    def findDistanceUsingDF(src: Monster, target: Monster): Int = {
        val distance  = dfEdge.select($"distance").where($"Monster1"("name") === src.name and $"Monster2"("name") === target.name)
        val result :Int = distance.collect()(0).getInt(0)
        return result
    }
    /*
    def findDistance(src: Monster, target: Monster) {

        val process = dfEdge.map( row=> (row.getInt(2))
        )
        rddEdges.foreach(element => {
            if (element.Monster1.name == src.name && element.Monster2.name == target.name) {
                var result = rddEdges.collect()(element.distance)
                println(result)


            }else
                {null}
        })

    }
*/
    //OK choix sort en fonction de la distaance entre monstre
    def SortChoice(monster: Monster, distance: Int):List[Sort]={
        var result = new Sort()
        var listSortResult = List.empty[Sort]
        for (i <- 0 until monster.listSort.length) {
            if(monster.listSort(i).distance >= distance) {
                result = monster.listSort(i)
                listSortResult = listSortResult:+result
            }
        }
        return listSortResult
    }

    def choice(monster: Monster, list : List[Sort]): Sort ={
        if(list != null){
            val r = new Random()
            val rand = r.nextInt(list.size)
            return list(rand)
        }
        null
    }

  /*  def attackOrRegen(monster: Monster): Boolean ={
        val r = new Random()
        val rand = r.nextInt(0)

    }*/

    def checkDead(monster : Monster, edges : RDD[edge]): Unit ={
        if(monster.HP == 0){
            var tmp = edges.collect().map{
                node => if(node.Monster1.id == monster.id || node.Monster2.id == monster.id){
                    (null, node.Monster2, node.distance)
                }

            /*var tmp = edges.collect().foreach{
                node => if(node.Monster1.id == monster.id || node.Monster2.id == monster.id){
                    node.Monster1 = null
                }*/
            }

            print()

        }
    }

    def attack(monster: Monster, target : Monster, edges : RDD[edge], msg : RDD[(Int, String)]): ArrayBuffer[(Int,String)] = {
        val r = new Random()
        var rand = 1 + r.nextInt(19)
        var messageMonster = new ArrayBuffer[(Int,String)]()
        val chosenSort = choice(monster,SortChoice(monster, findDistanceUsingDF(monster, target)))
        if(rand + chosenSort.listPower(monster.counterAtt) >= target.armure){
            messageMonster += Tuple2(monster.id,"test")
            monster.Attack(target,  chosenSort)
            if(target.HP == 0){
                messageMonster += Tuple2(target.id ,"dead")

            }
            else{
                messageMonster += Tuple2(target.id , target.HP.toString)
            }

            //checkDead(target, edges)
            print()

        }
        messageMonster.foreach(println)
        messageMonster
    }

    var myRDD = rddGraph.flatMap(monster => {
        val messageMonster = new ArrayBuffer[(Int, String)]
        messageMonster += Tuple2(1,"beau")
        messageMonster
    })


    /*
    myRDD.map(monster => {
        var id = monster._1
        var mymsg = sc.broadcast(id)

    })*/


    //println(findDistanceUsingDF(solar, warlord))
    //FindSorts_and_Attack(solar,warlord)
    //println(SortChoice(solar,findDistanceUsingDF(solar,warlord)))
    var msg = sc.makeRDD(attack(solar, worgs1, rddEdges,myRDD))
    var test = msg.join(myRDD).toDF()

    
    test.show()



    /*
    val test = myRDD.toDF()
    test.show()
*/




}
