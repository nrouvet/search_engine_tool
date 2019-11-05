
import org.apache.spark.sql.functions.size
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.io.Source

object main extends App {

  val conf = new SparkConf().setAppName("BDD2").setMaster("local[*]")
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
  result.collect().foreach(println)

  val regexHref = """<li><a href=".*"""".r

  val url = "http://legacy.aonprd.com/bestiary/"
  val index = "monsterIndex.html"

  val text = Source.fromURL(url+index)

  val htmlPage = text.mkString

  var listURL = regexHref.findAllIn(htmlPage).toList

  println(listURL)

  listURL(0)



}
