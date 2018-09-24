package part5

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import scala.util.parsing.json.JSON

object ReadWriteJson {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(conf)
    val jsonStr = sc.textFile("file:///E:\\project\\BigData\\Spark_1\\src\\main\\scala\\part5\\news.json")
    val result = jsonStr.map(s => JSON.parseFull(s))
    result.foreach(
      {
        r => r match {
          case Some (map:Map[String,Any])=> println(map)
          case None => println("无效的字段")
          case other => println("不知道的数据字段："+other)
        }
      }
    )
  }
}
