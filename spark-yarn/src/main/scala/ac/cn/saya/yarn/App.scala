package ac.cn.saya.yarn
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object App {
  def main(args: Array[String]) {
    // 设置提交任务的用户
//    System.setProperty("HADOOP_USER_NAME", "root")
//    val conf = new SparkConf()
//      .setAppName("WordCount")
//      // 设置yarn-client模式提交
//      .setMaster("yarn")
//      // 设置resourcemanager的ip
//      .set("yarn.resourcemanager.hostname","master")
//      // 设置executor的个数
//      .set("spark.executor.instance","1")
//      // 设置executor的内存大小
//      .set("spark.executor.memory", "1024M")
//      // 设置提交任务的yarn队列
//      .set("spark.yarn.queue","spark")
//      // 设置driver的ip地址
//      .set("spark.driver.host","10.203.2.102")
//      // 设置jar包的路径,如果有其他的依赖包,可以在这里添加,逗号隔开
//      .setJars(List("/Users/saya/project/java/spark-util/spark-yarn/target/spark-yarn-1.0-SNAPSHOT.jar"
//      ))
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    System.setProperty("HADOOP_USER_NAME", "hadoop")
    System.setProperty("user.name", "hadoop")

    val conf = new SparkConf().setAppName("WordCount").setMaster("yarn")
      //      // 设置resourcemanager的ip
      .set("yarn.resourcemanager.hostname","10.203.2.102")
      .set("deploy-mode", "client")
      .set("spark.yarn.queue","spark")
      .set("spark.yarn.jars", "hdfs:/user/hadoop/jars/*")  //集群的jars包,是你自己上传上去的
      .setJars(List("/Users/saya/project/java/spark-util/spark-yarn/target/spark-yarn-1.0-SNAPSHOT.jar"))//这是sbt打包后的文件
      .setIfMissing("spark.driver.host", "192.168.3.7") //设置你自己的ip

    // 创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(conf)

    val rdd : RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10))

    //将数据进行过滤，为true的才保留
    val rdd1 = rdd.filter(item => item%2==0)

    rdd1.collect().foreach(println)

    sc.stop()
  }
}
//https://www.jianshu.com/p/0a5f33fd45c8?utm_campaign=maleskine&utm_content=note&utm_medium=seo_notes&utm_source=recommendation