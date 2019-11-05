
import scala.io.StdIn.readLine
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import spire.std.string

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
    ("Solar", Array("etherealness", "mass", "heal", "miracle", "storm of vengeance","fire storm" ,"holy aura" ,"mass cure critical wounds","truck")),
    ("Bralani", Array("blur", "charm person", "gust of wind","mirror image", "wind wall","truck"))
  )

  val rdd = sc.makeRDD(monstre)

  val result = rdd.flatMap{
    case(monster, sorts)=>
      sorts.map(sort => (sort, monster))
  }
      .reduceByKey((a,b)=>(a+","+b))
      .mapValues(_.split(",").toArray)



  //println(result.collect()(0)._1)
  //result.collect().foreach(println)

  import session.implicits._
  val df = result.toDF("sort","monsters")
  df.show()


  val sorts_json = df.toJSON
  sorts_json.take(20).foreach(println)
  //dsorts_json.write.json("data/")


  val nameSort = readLine("enter sort's name: ")

  val reseachrSort = df.select($"monsters").filter($"sort"===nameSort)

  reseachrSort.show()



}
