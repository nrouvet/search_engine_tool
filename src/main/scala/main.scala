
import org.apache.spark.sql.functions.size
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.io.Source
import scala.util.matching.Regex

object main extends App {

  val url = "http://legacy.aonprd.com/bestiary/"  //link of the bestiary
  val index = "monsterIndex.html"   //part of the link to get the index of the monsters

  val regexHref = """<li><a href=".*"""".r
  val regexSpell = new Regex("""<a[^>]*\/spells\/[^>]*\#[^>]*>(\w+[^<]+)<\/a>""")  //Regex to get the spells
  val regexName ="""href="(\w+[a-z]*)""".r

  val text = Source.fromURL(url+index)
  val htmlPage = text.mkString    //Convert the HTML code of the page to a string
  var listURL = regexHref.findAllIn(htmlPage).toList    //Find all the href link of the page with the regex and make a list with the result

  var monster = Array.empty[(String, Array[String])] //To store the monsters and theirs sorts

  //We will browse all the links we get previously to get all the sorts of all the monsters
  for(i <- 0 until listURL.length){
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
    monster = monster :+ (nameMonster, listSpells.toArray)  //We create our couple of monster and their sorts and we add if to our list monster
  }

  println()
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
      .reduceByKey((a,b)=>(a+","+b))    //Then we reduce by key, so we group the monster by sort to inverse the couple to (sort, list(monster))
      .mapValues(_.split(",").toArray)  //We create an array with the string create by the reduceByKey

  //result.collect().foreach(println)
  /*for(i<-0 until result.collect().length){
    println(result.collect()(i)._1)
    println(result.collect()(i)._2.foreach(print))
    println()
  }*/


}
