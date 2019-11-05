
import org.apache.spark.sql.functions.size
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.io.Source
import scala.util.matching.Regex

object main extends App {

  val url = "http://legacy.aonprd.com/bestiary/"
  val index = "monsterIndex.html"

  val regexHref = """<li><a href=".*"""".r
  val regexSpell = new Regex("""<a[^>]*\/spells\/[^>]*\#[^>]*>(\w+[^<]+)<\/a>""")
  val regexName ="""\#(\w+[a-z]*)""".r

  val text = Source.fromURL(url+index)

  val htmlPage = text.mkString

  var listURL = regexHref.findAllIn(htmlPage).toList

  var monster = Array.empty[(String, Array[String])]

  for(i <- 0 until /*listURL.length*/1){
    val nameMonster = regexName.findFirstMatchIn(listURL(i)).get.group(1)
    val arrayUrl = listURL(i).split("\"")
    val getMonster = arrayUrl(1)
    val text2 = Source.fromURL(url+getMonster).mkString
    var listSpells = List[String]()
    regexSpell.findAllIn(text2).matchData.foreach{
      m =>
        if(!listSpells.contains( m.group(1)))   //Evite les doublons
          listSpells = listSpells:+ m.group(1)
    }
    monster = monster :+ (nameMonster, listSpells.toArray)
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
      sorts.map(sort => (sort, monster))
  }
      .reduceByKey((a,b)=>(a+","+b))
      .mapValues(_.split(",").toArray)



  //println(result.collect()(0)._1)
  result.collect().foreach(println)


}
