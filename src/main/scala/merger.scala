import java.io.{File, PrintWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lower

object merger extends App {

  def toJson(listSort:sql.DataFrame) = {
    var regex = """\((.*)\)""".r
    val pw = new PrintWriter(new File("merge.json"))
    pw.write("[\n")
    var size = listSort.collect().size
    for(i <- 0 until size){
      pw.write("{\"name\":\""+listSort.collect()(i)(0)+"\",\n\"monsters\":[\n")
      var monster = regex.findFirstMatchIn(merge.collect()(0)(1).toString).get.group(1).split(",")
      monster.foreach(x =>
        if(x == monster(monster.length-1)){
          pw.write("\"" + x + "\"],\n")
        }
        else pw.write("\"" + x + "\",")
      )
      pw.write("\"components\":[\n")
      var compo = regex.findFirstMatchIn(merge.collect()(0)(2).toString).get.group(1).split(",")
      compo.foreach(x =>
        if(x == compo(compo.length-1)){
          pw.write("\"" + x + "\"],\n")
        }
        else pw.write("\"" + x + "\",")
      )
      pw.write("\"level\":"+merge.collect()(0)(3) +",\n")
      pw.write("\"spell resitance\":\""+merge.collect()(0)(4)+"\"")
      if(i == size - 1) pw.write("\n}\n")
      else pw.write("\n},\n")
    }
    pw.write("]")
    pw.close()
  }

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


  var file1 = session.read.json("sort_spark.json").toDF()

  var tmp = session.read.json("sort_tp1.json").toDF()
  var file2 = tmp.withColumn("name", lower(tmp.col("name")))

  var merge = file1.join(file2, "name")

  toJson(merge)

}