
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

  println("Hello World")

}
