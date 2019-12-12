import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random


object main extends App {

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





  var solar = Monster(1, "Solar", "A", 50, 50, List(testAttackProche, testAttackLoin), 4)

  teamA.addMonster(solar)


  println("Début de la partie")
  println("************************")


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

  var rddGraph = sc.makeRDD(graph)

  import session.implicits._

  var dfGraph = rddGraph.toDF()

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

    if(monster.HP == 0 | target.HP == 0 | monster.counterAtt == monster.maxAtt) return Tuple2(target.id, (target, number))
    if (monster.equipe != target.equipe) {
      val r = new Random()
      var rand = 1 + r.nextInt(19)
      val chosenSort = choice(monster, SortChoice(monster))
      if (chosenSort == null) {

        println(monster.name + " ne peut pas attaquer ! Il est trop loin de " + target.name + "!")
      }
      else if (rand + chosenSort.listPower(monster.counterAtt) >= target.armure || rand==20) {

        monster.Attack(target, chosenSort)
        println(monster.name + " utilise " + chosenSort.name + " sur " + target.name)
        if (target.HP == 0) {

          println(target.name + " est mort !")
        }
        else {

        }
      }
      else {
        println(target.name+ " a parré l'attque de " + monster.name)
      }
    }

    Tuple2(target.id, (target,number))
  }

  println("début du combat ")
  println("*****************")


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
    }

    rddGraph = rddGraph.union(rddMessageTest)
      .reduceByKey((a, b) => if (a._1.HP < b._1.HP) a else b)


    rddGraph.toDF().show(false)


    var idMort = rddMessageTest.filter(f=> f._2._1.HP == 0).flatMap(monster => {
      Array(monster._2._1.id)
    }).collect()

    broad.value.foreach(x => x._2._1.counterAtt = 0)


    println("tour: "+ i)
    removeTeam(teamA, idMort)
    removeTeam(teamB, idMort)

    println("equipe")
    println("monstre dans chaque équipe après "+i+" tour")

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






}
