import exo2.{graph, sc, session}
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object testNico extends App {

  val conf = new SparkConf()
    .setAppName("BDDExo2")
    .setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val session = SparkSession
    .builder
    .appName("truck")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  var testAttackProche = Sort("sword", false, List(35, 30, 25, 20), 50, 3, 6)
  var testAttackLoin = Sort("elbow", false, List(30, 25, 20), 120, 2, 6)

  var hit = Sort("hit", false, List(10), 50, 3, 6)

  var listSort = List(testAttackLoin, testAttackProche)

  println(listSort)

  var teamA = new Team()
  var teamB = new Team()

  var worgs1 = (Monster(4, "Worgs Rider", "B", 33, 20, List(hit), 1))
  var warlord = (Monster(2, "Le Warlord", "B", 33, 20, List(hit), 1))
  var barbare = (Monster(3, "Barbares Orc", "B", 33, 20, List(hit), 1))

  teamB.addMonster(worgs1)
  teamB.addMonster(warlord)
  teamB.addMonster(barbare)

  println("listMonstreTeamB")
  println(teamB.monsters.size)

  teamB.monsters.foreach(println)



  var solar = (Monster(1, "Solar", "A", 50, 50, List(testAttackProche, testAttackLoin), 4))

  teamA.addMonster(solar)

  var graph = Array((solar, Array(2, 3, 4)), (warlord, Array(1, 4)), (barbare, Array(1, 4)), (worgs1, Array(1, 2, 3)))

  var edges = Array.empty[edge]
  //var edges = Array((1,(solar, worgs, 50)), (2, (solar, warlord, 50)), (3, (solar, barbare, 110)),
  //      (4, (worgs, warlord, 50)), (5, (warlord, barbare, 50)))

  for (i <- 0 until graph.length) {
    for (j <- 0 until graph(i)._2.length) {
      if (graph(graph(i)._2(j) - 1)._1.equipe == graph(i)._1.equipe) {
        var tmp = edge(graph(i)._1, graph(graph(i)._2(j) - 1)._1, 50)
        edges = edges :+ tmp
      }
      else {
        var tmp = edge(graph(i)._1, graph(graph(i)._2(j) - 1)._1, 110)
        edges = edges :+ tmp
      }
    }
  }


  var rddEdges = sc.makeRDD(edges)
  var rddGraph = sc.makeRDD(graph)

  //println(rddGraph.collect()(0))
  //println()

  //print result pour test attack

  import session.implicits._

  var dfGraph = rddGraph.toDF()
  var dfEdge = rddEdges.toDF()
  //dfGraph.show(200,false)
  dfEdge.show(200, false)
  //solar.Attack(warlord,testAttackProche)//OK
  //rddGraph.filter(monster => monster._1.HP>0)


  //dfGraph.printSchema()
  //val process = dfEdge.map( row=> (row.getInt(2)))
  //process.show()


  //OK retourne la distance entre deux monstre
  def findDistanceUsingDF(src: Monster, target: Monster): Int = {
    dfEdge.show()
    val distance = dfEdge.select($"distance").where($"Monster1"("name") === src.name and $"Monster2"("name") === target.name)
    val result: Int = distance.collect()(0).getInt(0)
    return result
  }

  def findDistance(src: Monster, target: Monster): Int = {
    var distance = rddEdges.filter(node => node.Monster1.name == src.name & node.Monster2.name == target.name).collect()(0).distance
    distance
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
  def SortChoice(monster: Monster, distance: Int): List[Sort] = {
    var result = new Sort()
    var listSortResult = List.empty[Sort]

    for (i <- 0 until monster.listSort.length) {
      if (monster.listSort(i).distance >= distance) {
        result = monster.listSort(i)
        listSortResult = listSortResult :+ result
      }
    }
    return listSortResult
  }

  def choice(monster: Monster, list: List[Sort]): Sort = {
    if (!list.isEmpty) {
      val r = new Random()
      val rand = r.nextInt(list.size)
      return list(rand)
    }
    null
  }

  def attackOrRegen(monster: Monster): Boolean = {
    val r = new Random()
    val rand = r.nextInt(100)
    val odd = monster.HP * 100 / monster.maxHP
    if (odd + 20 < rand) {
      true
    }
    else false
  }

  def attack(monster: Monster, target: Monster, edges: RDD[edge]): ArrayBuffer[(Monster, String)] = {
    val r = new Random()
    var rand = 1 + r.nextInt(19)
    var messageMonster = new ArrayBuffer[(Monster, String)]()
    val chosenSort = choice(monster, SortChoice(monster, findDistance(monster, target)))
    if (chosenSort == null) messageMonster += Tuple2(monster, monster.name + " ne peut pas attaquer ! Il est trop loin de " + target.name + "!")
    else if (rand + chosenSort.listPower(monster.counterAtt) >= target.armure) {
      messageMonster += Tuple2(monster ," attaque " )
      messageMonster += Tuple2(monster," utilise " + chosenSort.toString)
      monster.Attack(target, chosenSort)
      if (target.HP == 0) {
        messageMonster += Tuple2(target , "mort")

      }
      else {
        messageMonster += Tuple2(target, target.HP.toString)
      }
    }
    else messageMonster += Tuple2(target, target.name + " a parré l'attque de " + monster.name)
    //messageMonster.foreach(println)
    messageMonster
  }

  var myRDD = rddGraph.flatMap(monster => {
    val messageMonster = new ArrayBuffer[(Monster, String)]
    messageMonster :+ Tuple2(Monster, null)
    messageMonster
  })
  println("message")
  myRDD.toDF().show()

  /*
  myRDD.map(monster => {
      var id = monster._1
      var mymsg = sc.broadcast(id)

  })*/


  //println(findDistanceUsingDF(solar, warlord))
  //FindSorts_and_Attack(solar,warlord)
  //println(SortChoice(solar,findDistanceUsingDF(solar,warlord)))
  //var msg = sc.makeRDD(attack(solar, worgs1, rddEdges))
  //var test = msg
  //    .union(myRDD)
  //    .reduceByKey((a,b)=>a+","+b)
  //    .mapValues(_.split(",").toArray)

  //  test.collect().foreach(println)

  /*var equipeA = 0
  var equipeB = 0

  graph.foreach{monster=>
    if(monster._1.equipe == "A") equipeA+=1
    else equipeB += 1
  }

  while(equipeA != 0 /*| equipeB != 0*/){
      var idCurrent = 4

      rddGraph.map{
          monster =>
              if(monster._1.id==idCurrent){

              }
      }
      equipeA-=1
  }*/



  var rdd = rddGraph.flatMap {
      case (monster, adj) => {
          adj.map {
              x => var message = attack(monster, rddGraph.collect()(x - 1)._1, rddEdges)
              message
          }
      }
  }

  println("test rdd message")
  rdd.collect().foreach(println)

  /*
        .union(myRDD)
        .reduceByKey((a, b) => a + "," + b)
        .mapValues(_.split(",").toArray)*/

  //println(attack(solar,warlord,rddEdges))


  println("message")
  var rddMessage = sc.makeRDD(attack(solar,warlord,rddEdges))
  rddMessage.toDF().show(truncate =false)


  println("test filter mort")
  var idMort =rddMessage.filter(f => f._2.contains("mort")).flatMap(monster => {
    Array(monster._1.id)
  }).collect()
  idMort.foreach(println)


  var index = 0
  while(index < teamB.monsters.length) {
    if(idMort.contains(teamB.monsters(index).id)){
      teamB.dead(teamB.monsters(index))
      index-=1
    }
    index+=1
  }
  index  =0

  while(index < teamA.monsters.length) {
    if(idMort.contains(teamA.monsters(index).id)){
      teamA.dead(teamA.monsters(index))
      index-=1
    }
    index+=1
  }


  println("listTeam B")
  teamB.monsters.foreach(println)



  //rddGraph.toDF().show(truncate = false)
  //rddEdges.toDF().show(truncate = false)

  println("test join rddGraph ")




    var contrib = rddGraph.leftOuterJoin(rddMessage).reduceByKey((a, b) => (b._1, Option(a._2 + "," + b._2)))
    contrib.toDF().show(false)



     //rddGraph.leftOuterJoin(contrib).toDF().show(false)


  //println("Rdd without dead Monster")
  //var newRdd = .filter(f=> !(f._2._2.contains("mort"))).toDF().show()

  /*
  var newRdd2 = newRdd.flatMap(x => {
    var id = Array(x._1.id)
    var idEdges = x._2._1
    var test = Array[Int]()
    idEdges.flatMap(y=>{
      if(id.contains(y))
        {
          test :+ y
        }
      test
    })
  }).toDF().show(false)
*/







  println("test")



}