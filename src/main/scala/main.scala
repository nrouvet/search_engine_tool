
import org.apache.spark.sql.functions.size
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.io.Source

object main extends App {

  /*val conf = new SparkConf().setAppName("BDD2").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val session = SparkSession
    .builder
    .appName("")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  val monstre = Array(
    ("Solar", Array("etherealness", "mass", "heal", "miracle", "storm of vengeance","fire storm" ,"holy aura" ,"mass cure critical wounds")),
    ("Bralani", Array("blur", "charm person", "gust of wind","mirror image", "wind wall"))
  )

  val rdd = sc.makeRDD(monstre)

  val result = rdd.flatMap{
    case(monster, sorts)=>
      sorts.map(sort => (sort, monster))
  }
      .reduceByKey((a,b)=>(a+","+b))
      .mapValues(_.split(",").toArray)



  //println(result.collect()(0)._1)
  result.collect().foreach(println)*/

  val regexHref = """<li><a href=".*"""".r
  val regexName ="\\#[a-z]*".r

  val url = "http://legacy.aonprd.com/bestiary/"
  val index = "monsterIndex.html"

  val text = Source.fromURL(url+index)

  val htmlPage = text.mkString

  var listURL = regexHref.findAllIn(htmlPage).toList
  for(i <- 0 until /*listURL.length*/1){
    val arrayUrl = listURL(i).split("\"")
    val monster = arrayUrl(1)
    val text2 = Source.fromURL(url+monster).mkString
    //println(text2)
  }

  //récupère tout les noms du monstre
  var listName = regexName.findAllIn(htmlPage).toList
  println(listName)
  /*for(i<-0 until 1){
    val arrayName = listName(i).split("#")
    val nameMonster = arrayName(1)
    println(nameMonster)
  }*/





}
