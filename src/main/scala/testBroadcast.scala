import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import testMessage.{graph, sc}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object testBroadcast extends App{
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

  var testAttackProche = Sort("sword", false, List(35, 30, 25, 20), 3, 6)
  var testAttackLoin = Sort("elbow", false, List(30, 25, 20), 2, 6)

  var hit = Sort("hit", false, List(10), 3, 6)

  var listSort = List(testAttackLoin, testAttackProche)

  //println(listSort)

  var teamA = new Team()
  var teamB = new Team()


  var warlord = Monster(2, "Le Warlord", "B", 33, 33, List(hit), 1)
  var barbare1 = Monster(3, "Barbares Orc1", "B", 33, 33, List(hit), 1)
  var barbare2 = Monster(4, "Barbares Orc2", "B", 33, 33, List(hit), 1)
  var barbare3 = Monster(5, "Barbares Orc3", "B", 33, 33, List(hit), 1)
  var barbare4 = Monster(6, "Barbares Orc4", "B", 33, 33, List(hit), 1)
  var worgs1 = Monster(7, "Worgs Rider1", "B", 33,33, List(hit), 1)
  var worgs2 = Monster(8, "Worgs Rider2", "B", 33, 33, List(hit), 1)
  var worgs3 = Monster(9, "Worgs Rider3", "B", 33, 33, List(hit), 1)
  var worgs4 = Monster(10, "Worgs Rider4", "B", 33, 33, List(hit), 1)
  var worgs5 = Monster(11, "Worgs Rider5", "B", 33, 33, List(hit), 1)
  var worgs6 = Monster(12, "Worgs Rider6", "B", 33, 33, List(hit), 1)
  var worgs7 = Monster(13, "Worgs Rider7", "B", 33, 33, List(hit), 1)
  var worgs8 = Monster(14, "Worgs Rider8", "B", 33, 33, List(hit), 1)
  var worgs9 = Monster(15, "Worgs Rider9", "B", 33, 33, List(hit), 1)


  teamB.addMonster(warlord)
  teamB.addMonster(barbare1)
  teamB.addMonster(barbare2)
  teamB.addMonster(barbare3)
  teamB.addMonster(barbare4)
  teamB.addMonster(worgs1)
  teamB.addMonster(worgs2)
  teamB.addMonster(worgs3)
  teamB.addMonster(worgs4)
  teamB.addMonster(worgs5)
  teamB.addMonster(worgs6)
  teamB.addMonster(worgs7)
  teamB.addMonster(worgs8)
  teamB.addMonster(worgs9)

  //println("listMonstreTeamB")
  //println(teamB.monsters.size)

  //teamB.monsters.foreach(println)



  var solar = Monster(1, "Solar", "A", 50, 50, List(testAttackProche, testAttackLoin), 4)

  teamA.addMonster(solar)

  //println("Monstre au début de la partie")
  println("Début de la partie")
  println("************************")
  //println("EquipeA")
  //teamA.monsters.foreach(x => println(x.name))
  //println("EquipeB")
  //teamB.monsters.foreach(x => println(x.name))

  var graph: Array[(Int, (Monster, Array[Int]))] = Array((1, (solar, Array(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15))),
    (2,(warlord, Array(1))),
    (3, (barbare1, Array(1))),
    (4, (barbare2, Array(1))),
    (5, (barbare3, Array(1))),
    (6, (barbare4, Array(1))),
    (7,(worgs1, Array(1))),
    (8, (worgs2, Array(1))),
    (9, (worgs3, Array(1))),
    (10, (worgs4, Array(1))),
    (11, (worgs5, Array(1))),
    (12, (worgs6, Array(1))),
    (13, (worgs7, Array(1))),
    (14, (worgs8, Array(1))),
    (15, (worgs9, Array(1)))
  )

  var broad = sc.broadcast(graph)
  //broad.value(0)._2._1.id=500


  var edges = Array.empty[edge]
  //var edges = Array((1,(solar, worgs, 50)), (2, (solar, warlord, 50)), (3, (solar, barbare, 110)),
  //      (4, (worgs, warlord, 50)), (5, (warlord, barbare, 50)))

  for (i <- 0 until graph.length) {
    for (j <- 0 until graph(i)._2._2.length) {
      if (graph(graph(i)._2._2(j) - 1)._2._1.equipe == graph(i)._2._1.equipe) {
        var tmp = edge(graph(i)._2._1, graph(graph(i)._2._2(j) - 1)._2._1, 20)
        edges = edges :+ tmp
      }
      else {
        var tmp = edge(graph(i)._2._1, graph(graph(i)._2._2(j) - 1)._2._1, 40)
        edges = edges :+ tmp
      }
    }
  }


  var rddEdges = sc.makeRDD(edges)
  var rddGraph = sc.makeRDD(graph)

  import session.implicits._

  var dfGraph = rddGraph.toDF()
  var dfEdge = rddEdges.toDF()


  def findDistance(src: Monster, target: Monster): Int = {
    var distance = rddEdges.filter(node => node.Monster1.name == src.name & node.Monster2.name == target.name).collect()(0).distance
    distance
  }

  //OK choix sort en fonction de la distaance entre monstre
  def SortChoice(monster: Monster): List[Sort] = {
    var result = new Sort()
    var listSortResult = List.empty[Sort]

    for (i <- 0 until monster.listSort.length) {
      if (monster.listSort(i).listPower.size > monster.counterAtt) {
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

  def attack(monster: Monster, target: Monster, number : Array[Int]): (Int, (Monster, Array[Int])) = {
    //var messageMonster = new ArrayBuffer[(Monster, String)]()
    if(monster.HP == 0 | target.HP == 0 | monster.counterAtt == monster.maxAtt) return Tuple2(target.id, (target, number))
    if (monster.equipe != target.equipe) {
      val r = new Random()
      var rand = 1 + r.nextInt(19)
      val chosenSort = choice(monster, SortChoice(monster))
      if (chosenSort == null) {
        //messageMonster += Tuple2(monster, monster.name + " ne peut pas attaquer ! Il est trop loin de " + target.name + "!")
        println(monster.name + " ne peut pas attaquer ! Il est trop loin de " + target.name + "!")
      }
      else if (rand + chosenSort.listPower(monster.counterAtt) >= target.armure || rand==20) {
        //messageMonster += Tuple2(monster, " attaque ")
        //messageMonster += Tuple2(monster, " utilise " + chosenSort.name)
        //println(monster.name + " attaque ")
        //println(monster.name + " utilise " + chosenSort.name)
        monster.Attack(target, chosenSort)
        println(monster.name + " utilise " + chosenSort.name + " sur " + target.name)
        if (target.HP == 0) {
          //messageMonster += Tuple2(target, "mort")
          println(target.name + " est mort !")
        }
        else {
          //messageMonster += Tuple2(target, target.HP.toString)
        }
      }
      else { //messageMonster += Tuple2(target, target.name + " a parré l'attque de " + monster.name)
        println(target.name+ " a parré l'attque de " + monster.name)
      }
    }
    //messageMonster.foreach(println)
    //messageMonster
    //if(target.id==1 && target.HP != 0) target.Heal(15);
    Tuple2(target.id, (target,number))
  }

  var myRDD = rddGraph.flatMap(monster => {
    val messageMonster = new ArrayBuffer[(Monster, String)]
    messageMonster :+ Tuple2(Monster, null)
    messageMonster
  })
  println("début du combat ")
  println("*****************")
  //myRDD.toDF().show()

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

  def removeTeam(team : Team, monster : Array[Int]) : Unit ={
    var index = 0
    while(index < team.monsters.length) {
      if(monster.contains(team.monsters(index).id)){
        team.dead(team.monsters(index))
        index-=1
      }
      index+=1
    }
    index  =0
  }


  var i = 1
  while(!teamB.hasLost() && !teamA.hasLost()){
    println("Reste dans l'équipe A : " + teamA.monsters.size)
    println("Reste dans l'équipe B : " +teamB.monsters.size)
    println()
    println("Messages : ")
    var rddMessageTest = rddGraph.flatMap {
      case (_, monster) => {
        monster._2.flatMap {
          x =>
            var message = attack(broad.value(monster._1.id - 1)._2._1, broad.value(x - 1)._2._1, broad.value(x - 1)._2._2)
            Array(message)
        }
      }
    }//.reduceByKey((a, b) => if (a._1.HP < b._1.HP) a else b)
    //rddMessageTest.reduceByKey((a, b) => a._1.HP = a._1.maxHP - (a._1.maxHP - a._1.HP) - (b._1.maxHP - b._1.HP))

    //println("Fin des messages")
    /*var rddMessageTest = rddGraph
    for(i <- 0 until graph.length){
        rddMessageTest.union(sc.makeRDD(graph(i)._2._2.flatMap {
          x =>
            var message = attack(broad.value(i)._2._1, broad.value(x-1)._2._1, rddEdges, broad.value(x-1)._2._2)
            Array(message)
        }))
      }*/

    //rddMessageTest.localCheckpoint()
    /*for(i <- 0 to 4){
      var t = graph(0)._2._2.flatMap{
       x=> var message=attack(graph(0)._2._1, broad.value(x-1)._2._1, rddEdges, broad.value(x-1)._2._2)
          Array(message)
      }
    }*/

    //rddMessageTest.localCheckpoint()
    //rddMessageTest.toDF().show(0,false)  //Déclencheur

    /*for(i <- 0 to 4){
      var t = graph(0)._2._2.flatMap{
        x=> var message=attack(graph(0)._2._1, broad.value(x-1)._2._1, rddEdges, broad.value(x-1)._2._2)
          Array(message)
      }
    }*/

    //rddGraph.toDF().show(false)
    //rddMessageTest.localCheckpoint()
    rddGraph = rddGraph.union(rddMessageTest)
      .reduceByKey((a, b) => if (a._1.HP < b._1.HP) a else b)

    //rddGraph.localCheckpoint()
    rddGraph.toDF().show(false)

    //rddGraph.collect().foreach(println)
    var idMort = rddMessageTest.filter(f=> f._2._1.HP == 0).flatMap(monster => {
      Array(monster._2._1.id)
    }).collect()

    //var tmp = rddGraph

    //broad = sc.broadcast(tmp.collect())
    broad.value.foreach(x => x._2._1.counterAtt = 0)
    //rddGraph = rddGraph.map{ x => x._2._1.counterAtt = 0}

    println("tour: "+ i)
    removeTeam(teamA, idMort)
    removeTeam(teamB, idMort)

    println("equipe")
    println("monstre dans chaque équipe après "+i+" tour")
    //teamB.monsters.foreach(x => println(x.name))
    //teamA.monsters.foreach(x => println(x.name))
    println("****************")

    i += 1
  }

  if(teamA.hasLost()){
    println("L'équipe A a perdu. Il reste dans l'équipe B:")
    teamB.monsters.foreach(x => println(x.name))
  }

  if(teamB.hasLost()){
    println("L'équipe B a perdu. Il reste dans l'équipe A:")
    teamA.monsters.foreach(x => println(x.name))
  }


  //println("test rdd message")
  //rdd.collect().foreach(println)

  /*
        .union(myRDD)
        .reduceByKey((a, b) => a + "," + b)
        .mapValues(_.split(",").toArray)*/

  //println(attack(solar,warlord,rddEdges))


  //println("message")
  //var rddMessage = sc.makeRDD(attack(solar,warlord,rddEdges))
  //rddMessage.toDF().show(truncate =false)
  /*

    //println("test filter mort")
    var idMort = rddMessageTest.filter(f => f._2.contains("mort")).flatMap(monster => {
      Array(monster._1)
    }).collect()
    //idMort.foreach(println)
    //println("new rdd")
    var newRdd = rddMessageTest.filter(f=> !f._2.contains("mort")).map(monster => {
      monster._1
    }).collect()

    newRdd.foreach(println)
  */


  //removeTeam(teamA, idMort)
  //removeTeam(teamB, idMort)

  /*
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
    }*/


  //println("listTeam B")
  //teamB.monsters.foreach(println)



  //rddGraph.toDF().show(truncate = false)
  //rddEdges.toDF().show(truncate = false)

  // println("test join rddGraph ")


  //var newRddGraph = rddGraph.keyBy(key=>key._1.id)

  //println("Messages:")
  //rddMessageTest.collect().foreach(println)
  //var contrib = rddGraph.join(rddMessageTest)
  //println("contrib")
  //contrib.toDF().show(false)



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
  /*
    var extract = contrib.map{
      case(monster, tuple) => tuple
    }
        .map{
          case(link, msg) => link
        }.collect()

    var tmp = rddGraph.collect()
    //tmp = sc.makeRDD(tmp)



    def finalRDD(array: Array[Array[Int]], array2: Array[(Monster, Array[Int])]) : Array[(Monster, Array[Int])] = {
      var t : ArrayBuffer[(Monster,Array[Int])] = ArrayBuffer()
      for(i <- 0 until array.length){
        t += Tuple2(array2(i)._1, array(i))
      }
      t.toArray
    }

  var finl = sc.makeRDD(finalRDD(extract, tmp))

    rddGraph = finl
    //rddGraph = rddGraph.join(finl)
    //println(finl.collect())
    finl.collect().foreach(println)
    println("bo")
    rddGraph.collect().foreach(println)

    println("test")

*/
}
