import java.io.{File, PrintWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.matching.Regex

object crawler extends App {

  def createListMonster(url : String, listURL : List[String]) : Array[(String, Array[String])] = {
    var monster = Array.empty[(String, Array[String])]

    //Regex part 2
    val regexName ="""#([a-z\s\-\,]*)""".r
    val regexSpell = new Regex("""<a[^>]*\/spells\/[^>]*\#[^>]*>(\w+[^<]+)<\/a>""")  //Regex to get the spells

    //We will browse all the links we get previously to get all the sorts of all the monsters
    for(i <- 0 until listURL.length){
      try{
      val nameMonster = regexName.findFirstMatchIn(listURL(i)).get.group(1)   //We have a create a group with the regex in the brackets, we get the result with "get.group(1)
      val arrayUrl = listURL(i).split("\"")  //Here we get the value of the link without the href or the quotation marks
      val getMonster = arrayUrl(1)
      val text2 = Source.fromURL(url+getMonster).mkString  //We get the HTLM code of the page of the current monster
      var listSpells = List[String]()
      regexSpell.findAllIn(text2).matchData.foreach{  //We get all the sorts display on the page of the current monster with the regex
        m =>
          if(!listSpells.contains( m.group(1)))   //Avoid duplication
            listSpells = listSpells:+ m.group(1)
      }
      monster = monster :+ (nameMonster, listSpells.toArray) } //We create our couple of monster and their sorts and we add if to our list monster
      catch {
        case _: Throwable => println("error")
        println(i)
      }
    }
    monster
  }

  def toJson(listSort : RDD[(String, Array[String])]) = {
    val pw = new PrintWriter(new File("sort.json"))
    pw.write("[\n")
    for(i <- 0 until listSort.collect().size){
      pw.write("{\"name\":\""+listSort.collect()(i)._1+"\",\n\"monsters\":[\n")
      for(j<-0 until listSort.collect()(i)._2.size){
        if(j == listSort.collect()(i)._2.size-1) {
          pw.write("\""+listSort.collect()(i)._2(j) + "\"")
        }
        else pw.write("\"" + listSort.collect()(i)._2(j) + "\",\n")
      }
      if(i == listSort.collect().length-1) {
        pw.write("\n]\n}\n")
      }
      else
        pw.write("\n]\n},\n")
    }
    pw.write("]")
    pw.close()
  }

  def toJsonSpark(listSort : RDD[(String, Array[String])]) = {
    val pw = new PrintWriter(new File("sort_spark.json"))
    for(i <- 0 until listSort.collect().size){
      pw.write("{\"name\":\""+listSort.collect()(i)._1+"\",\"monsters\":[")
      for(j<-0 until listSort.collect()(i)._2.size){
        if(j == listSort.collect()(i)._2.size-1) {
          pw.write("\""+listSort.collect()(i)._2(j) + "\"")
        }
        else pw.write("\"" + listSort.collect()(i)._2(j) + "\",")
      }
      pw.write("]}\n")
    }
    pw.close()
  }

  def removeDuplicateAndDifferentesType(bufferURL : mutable.Buffer[String], bufferURL2 : mutable.Buffer[String]): List[String] ={
    var monsterType = """(\w*).html""".r
    for(i <- 0 until bufferURL2.size){
      bufferURL2(i) = bufferURL2(i).replace("-", " ")
      listType = listType :+ monsterType.findFirstMatchIn(bufferURL2(i)).get.group(1).toString
    }

    listType = listType.distinct

    var ind = 0
    for(i <- 0 until bufferURL.size){
      var checkDuplicate = monsterType.findFirstMatchIn(bufferURL(ind)).get.group(1).toString
      if(listType.contains(checkDuplicate)){
        bufferURL -= bufferURL(ind)
        ind -= 1
      }
      ind+=1
    }

    var listURL = bufferURL.toList
    listURL = listURL ++ bufferURL2.toList

    listURL
  }

  var monster = Array.empty[(String, Array[String])] //To store the monsters and theirs sorts
  var listURL = List.empty[String]
  var listType = List.empty[String]

  //Regex part
  var regexHref = """<li><a href=".*"""".r
  var regexDifferentTypeOfSameMonster = """<li>&nbsp;&nbsp;<a href=".*"""".r

  //Page 1
  var url = "http://legacy.aonprd.com/bestiary/"  //link of the bestiary
  var index = "monsterIndex.html"   //part of the link to get the index of the monsters

    var text = Source.fromURL(url+index)
    var htmlPage = text.mkString    //Convert the HTML code of the page to a string
    var bufferURL = regexHref.findAllIn(htmlPage).toBuffer    //Find all the href link of the page with the regex and make a buffer with the result

    var bufferURL2 = regexDifferentTypeOfSameMonster.findAllIn(htmlPage).toBuffer

    listURL = removeDuplicateAndDifferentesType(bufferURL, bufferURL2)

    monster = monster ++ createListMonster(url, listURL)

    println("Page 1 ok")

  //Page 2
  url = "http://legacy.aonprd.com/bestiary2/"
  index = "additionalMonsterIndex.html#"

  text = Source.fromURL(url+index)
  htmlPage = text.mkString
  bufferURL = regexHref.findAllIn(htmlPage).toBuffer

  bufferURL2 = regexDifferentTypeOfSameMonster.findAllIn(htmlPage).toBuffer

  listURL = removeDuplicateAndDifferentesType(bufferURL, bufferURL2)

  monster = monster ++ createListMonster(url, listURL)

  println("Page 2 ok")

  //Page 3 et 4
  for(i <- 3 to 4){
    url = "http://legacy.aonprd.com/bestiary"+i+"/"
    index = "monsterIndex.html"

    text = Source.fromURL(url+index)
    htmlPage = text.mkString
    bufferURL = regexHref.findAllIn(htmlPage).toBuffer

    bufferURL2 = regexDifferentTypeOfSameMonster.findAllIn(htmlPage).toBuffer

    listURL = removeDuplicateAndDifferentesType(bufferURL, bufferURL2)

    monster = monster ++ createListMonster(url, listURL)

    println("Page " + i +" ok")
  }

  //Page 5
  regexHref = """<li><a href\s?=\s?".*#.*"""".r

  url = "http://legacy.aonprd.com/bestiary5/"
  index = "index.html"

  text = Source.fromURL(url+index)
  htmlPage = text.mkString
  listURL = regexHref.findAllIn(htmlPage).toList

  monster = monster ++ createListMonster(url, listURL)

  println("Page 5 ok")

  val conf = new SparkConf().setAppName("BDD2").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val session = SparkSession
    .builder
    .appName("")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()


  val rdd = sc.makeRDD(monster)

  val result = rdd.flatMap{
    case(monster, sorts)=>
      sorts.map(sort => (sort, monster))  //For all the sort of each monster, we create a couple of (sort,monster)
  }
    .reduceByKey((a,b)=>(a+"!"+b))    //Then we reduce by key, so we group the monster by sort to inverse the couple to (sort, list(monster))
    .mapValues(_.split("!").toArray)  //We create an array with the string create by the reduceByKey

  //println("Starting write Spark format json...")
  //toJsonSpark(result)
  //println("Spark format json ok")

  println("Starting write json...")
  toJson(result)
  println("Json ok")


  //result.collect().foreach(println)
  /*for(i<-0 until result.collect().length){
    println(result.collect()(i)._1)
    println(result.collect()(i)._2.foreach(print))
    println()
  }*/

  //println(result.collect()(0)._1)
  //result.collect().foreach(println)


  import session.implicits._
  val df = result.toDF("sort","monsters")
  df.show(200,true)


  val sorts_json = df.toJSON
  //sorts_json.take(20).foreach(println)
  //sorts_json.write.json("data/")


  val nameSort = readLine("enter sort's name: ")

  val reseachrSort = df.select($"monsters").filter($"sort"===nameSort)

  reseachrSort.show()

}
