
import org.apache.spark.sql.functions.size
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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

  println("Hello World")

}
