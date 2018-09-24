package part5

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object ReadWriteText {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("file:///E:\\linshi\\hadoop\\workCount\\in\\text.txt")
    //val words = lines.flatMap(x => x.split(" ")).countByValue()
    val words = lines.flatMap(line => line.split(" "))
    val count =  words.map(word => (word,1)).reduceByKey((x,y) => x+y)
    count.saveAsTextFile("file:///E:\\linshi\\hadoop\\workCount\\out")

  }
}
