import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object testMessage extends App {
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

  //println(listSort)

  var teamA = new Team()
  var teamB = new Team()

  var worgs1 = (Monster(4, "Worgs Rider", "B", 33,150, List(hit), 1))
  var warlord = (Monster(2, "Le Warlord", "B", 33, 150, List(hit), 1))
  var barbare = (Monster(3, "Barbares Orc", "B", 33, 150, List(hit), 1))

  teamB.addMonster(worgs1)
  teamB.addMonster(warlord)
  teamB.addMonster(barbare)

  //println("listMonstreTeamB")
  //println(teamB.monsters.size)

  //teamB.monsters.foreach(println)



  var solar = (Monster(1, "Solar", "A", 50, 50, List(testAttackProche, testAttackLoin), 4))

  teamA.addMonster(solar)

  println("Monstre au début de la partie")
  println("EquipeA")
  teamA.monsters.foreach(println)
  println("EquipeB")
  teamB.monsters.foreach(println)

  var graph = Array((1, (solar, Array(2, 3, 4))), (2,(warlord, Array(1, 4))), (3, (barbare, Array(1, 4))), (4,(worgs1, Array(1, 2, 3))))

  var edges = Array.empty[edge]
  //var edges = Array((1,(solar, worgs, 50)), (2, (solar, warlord, 50)), (3, (solar, barbare, 110)),
  //      (4, (worgs, warlord, 50)), (5, (warlord, barbare, 50)))

  for (i <- 0 until graph.length) {
    for (j <- 0 until graph(i)._2._2.length) {
      if (graph(graph(i)._2._2(j) - 1)._2._1.equipe == graph(i)._2._1.equipe) {
        var tmp = edge(graph(i)._2._1, graph(graph(i)._2._2(j) - 1)._2._1, 50)
        edges = edges :+ tmp
      }
      else {
        var tmp = edge(graph(i)._2._1, graph(graph(i)._2._2(j) - 1)._2._1, 110)
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
  //dfEdge.show(200, false)
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

  def attack(monster: Monster, target: Monster, edges: RDD[edge], number : Array[Int]): (Int, (Monster, Array[Int])) = {
    //var messageMonster = new ArrayBuffer[(Monster, String)]()
    if(monster.HP == 0 | target.HP == 0) return Tuple2(target.id, (target, number))
    if (monster.equipe != target.equipe) {
      val r = new Random()
      var rand = 1 + r.nextInt(19)
      val chosenSort = choice(monster, SortChoice(monster, findDistance(monster, target)))
      if (chosenSort == null) {
        //messageMonster += Tuple2(monster, monster.name + " ne peut pas attaquer ! Il est trop loin de " + target.name + "!")
        println(monster.name + " ne peut pas attaquer ! Il est trop loin de " + target.name + "!")
      }
      else if (rand + chosenSort.listPower(monster.counterAtt) >= target.armure) {
        //messageMonster += Tuple2(monster, " attaque ")
        //messageMonster += Tuple2(monster, " utilise " + chosenSort.name)
        //println(monster.name + " attaque ")
        //println(monster.name + " utilise " + chosenSort.name)
        monster.Attack(target, chosenSort)
        if (target.HP == 0) {
          //messageMonster += Tuple2(target, "mort")

        }
        else {
          //messageMonster += Tuple2(target, target.HP.toString)
        }
      }
      //else messageMonster += Tuple2(target, target.name + " a parré l'attque de " + monster.name)
    }
    //messageMonster.foreach(println)
    //messageMonster
    Tuple2(target.id, (target,number))
  }

  var myRDD = rddGraph.flatMap(monster => {
    val messageMonster = new ArrayBuffer[(Monster, String)]
    messageMonster :+ Tuple2(Monster, null)
    messageMonster
  })
  println("message")
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


  var i = 1
  while(teamA.monsters.size + teamB.monsters.size > 1){
    println(teamA.monsters.size)
    println(teamB.monsters.size)
    var rddMessageTest = rddGraph.flatMap {
      case (_, monster) => {
        monster._2.flatMap {
          x =>
            var message = attack(monster._1, rddGraph.collect()(x - 1)._2._1, rddEdges, rddGraph.collect()(x - 1)._2._2)
            Array(message)
        }
      }
    }.reduceByKey((a, b) => if (a._1.HP < b._1.HP) a else b)

    rddMessageTest.toDF().show(false)

    //rddGraph.toDF().show(false)

     rddGraph = rddGraph.union(rddMessageTest)
      .reduceByKey((a, b) => if (a._1.HP < b._1.HP) a else b)

    rddGraph.collect().foreach(println)
    var idMort = rddMessageTest.filter(f=> f._2._1.HP == 0).flatMap(monster => {
      Array(monster._2._1.id)
    }).collect()

    println("tour: "+ i)
    removeTeam(teamA, idMort)
    removeTeam(teamB, idMort)

    println("equipe")
    println("monstre dans chaque équipe après "+i+" tour")
    teamB.monsters.foreach(println)
    teamA.monsters.foreach(println)

    i += 1
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
