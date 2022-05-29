package ac.cn.saya

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

/**
 * 自定义的采集器
 */
object MyReceiver {

  def main(args: Array[String]): Unit = {
    //1.初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDStream")

    //2.初始化SparkStreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //3.创建采集器
    val lineStream: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("127.0.0.1",9000))

    //4.将每一行数据做切分，形成一个个单词
    val wordStream: DStream[String] = lineStream.flatMap(line => line.split(" "))

    //5.将单词映射成元组（word,1）
    val wordAndOneStream = wordStream.map((_, 1))

    //6.将相同的单词次数做统计
    val wordAndCountStream = wordAndOneStream.reduceByKey(_ + _)

    //7.打印
    wordAndCountStream.print()

    //8.启动任务
    ssc.start()
    //9.等待采集器的关闭
    ssc.awaitTermination()

  }

  class CustomerReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

    // 最初启动的时候，调用该方法，读数据并将数据发送给spark
    override def onStart(): Unit = {
      new Thread("Socket Receiver") {
        override def run(): Unit = {
         receive()
        }
      }.start()
    }

    // 读数据并将数据发送给spark
    def receive():Unit = {
      // 创建一个Socket
      val socket: Socket = new Socket(host, port)

      // 定义一个变量，用于接收端口传过来的数据
      var input: String = null

      // 创建一个BufferReader用于读取端口传来的数据
      val reader: BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))

      // 读取数据
      reader.readLine()

      // 当receive没有关闭并且输入数据不为空，则循环发送数据给spark
      while (!isStopped() && input != null) {
        store(input)
        input = reader.readLine()
      }

      // 跳出循环则关闭资源
      reader.close()
      socket.close()

      // 重启任务
      restart("restart")
    }

    override def onStop(): Unit = {
      println("CustomerReceiver is stopped")
    }
  }


}
