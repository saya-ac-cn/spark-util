package ac.cn.saya

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}


/**
 * Stream 优雅关闭
 */
object StreamClose {

  def main(args: Array[String]): Unit = {
    // 恢复数据，如果没有则创建
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck", () => createSSC())
    new Thread(new MonitorStop(ssc)).start()
    ssc.start()
    ssc.awaitTermination()
  }

  def createSSC():_root_.org.apache.spark.streaming.StreamingContext ={
    val update: (Seq[Int], Option[Int]) => Some[Int] = (values: Seq[Int], status: Option[Int]) => {
      //当前批次内容的计算
      val sum: Int = values.sum
      //取出状态信息中上一次状态
      val lastStatu: Int = status.getOrElse(0)
      Some(sum + lastStatu)
    }
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkTest")
    //设置优雅的关闭
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(sparkConf, Seconds(4))
    ssc.checkpoint("./ck")
    val line: ReceiverInputDStream[String] = ssc.socketTextStream("127.0.0.1", 9999)
    val word: DStream[String] = line.flatMap(_.split(" "))
    val wordAndOne: DStream[(String, Int)] = word.map((_, 1))
    val wordAndCount: DStream[(String, Int)] = wordAndOne.updateStateByKey(update)
    wordAndCount.print()
    ssc
  }

  /**
   * 使用hdfs作为中间载体，不停的去监听文件，一旦发现有，则停止服务
   * @param ssc
   */
  class MonitorStop(ssc: StreamingContext) extends Runnable {
    override def run(): Unit = {
      val fs: FileSystem = FileSystem.get(new URI("hdfs://127.0.0.1:9000"), new Configuration(), "saya")
      while (true){
        try{
          Thread.sleep(5000)
        }catch {
          case e: Exception => e.printStackTrace()
        }
        val state: StreamingContextState = ssc.getState()
        val bool: Boolean = fs.exists(new Path("hdfs://127.0.0.1:9000/stopSpark"))
        if (bool && state == StreamingContextState.ACTIVE) {
            ssc.stop(stopSparkContext = true, stopGracefully = true)
            System.exit(0)
        }
      }
    }
  }


}
