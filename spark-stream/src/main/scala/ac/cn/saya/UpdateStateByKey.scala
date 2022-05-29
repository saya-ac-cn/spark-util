package ac.cn.saya

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateByKey {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(3))
    // 使用有状态操作时，需要设置缓冲区路径
    ssc.checkpoint("./ck")

    // Create a DStream that will connect to hostname:port, like hadoop102:9999
    val lines = ssc.socketTextStream("127.0.0.1", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))


    val pairs = words.map(word => (word, 1))

    // 定义更新状态方法，参数values为当前批次单词频度，state为以往批次单词频度
    // 第一个值表示相同key的value数据，第二个表示缓存区域相同key的value数据
    val updateFunc = (seq: Seq[Int], buff: Option[Int]) => {
      val currentCount = seq.sum
      val previousCount = buff.getOrElse(0)
      Option(currentCount + previousCount)
    }


    // 使用updateStateByKey来更新状态，统计从运行开始以来单词总的次数
    val stateDstream = pairs.updateStateByKey[Int](updateFunc)
    stateDstream.print()

    ssc.start()
    ssc.awaitTermination()


  }

}
