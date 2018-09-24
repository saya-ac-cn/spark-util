package ft

import scala.collection.mutable.Stack

object ReadCSV {


  def main(args: Array[String]): Unit = {

    //首先初始化一个SparkSession对象
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .config("spark.sql.warehouse.dir","file:///E:\\project\\BigData\\Spark_1\\src\\main\\scala\\ft\\spark-warehouse")
      .getOrCreate;

    //然后使用SparkSessions对象加载CSV成为DataFrame
    val csvRdd = spark.read
      .format("csv")
      .option("header", "false") //reading the headers
      .load("file:///E:\\project\\BigData\\Spark_1\\src\\main\\scala\\ft\\doc_log.csv");

    val groupRdd = csvRdd.rdd.filter(row => {
      var flog:Boolean = false;
      if(row.getAs(0) == "0" || row.getAs(0) == "1" || row.getAs(0) == "2" || row.getAs(0) == "3"|| row.getAs(0) == "4" || row.getAs(0) == "7")
        flog = true
        flog;
    }).groupBy(f => f(2))

    groupRdd.collect().foreach(println)

    var beginTime:Long = 0;//记录开始时间
    var endTime:Long  = 0;//记录结束时间
    var countSecond:Long  = 0;//存贮时长

    var i = 0;

    //医生在线时长rdd
    val online_doc_rdd = groupRdd.map(f =>{
      var map = scala.collection.mutable.Map("account" -> f._1, "online_time" -> 0)
      val iterable = f._2;
      if(iterable.size < 1){
        map("online_time") = 0;
      }else{
        val stack = new Stack[String];//初始化栈
        var last_login_time:Long = 0;//最后一次登录时间
        var count_second:Long = 0;//统计时长
        var i = 0
        var tem_list = iterable.toList
        tem_list.foreach(elem => {
          // 登录
          if(elem(0) == "0")
          {
            stack.push(elem(0).toString);
            last_login_time = elem(1).toString.toLong;
          }
          // 登出、异常登出
          if(elem(0) == "1" || elem(0) == "2")
          {
            if(stack.size != 0 && stack.top == "0")
            {
              count_second = count_second + (elem(1).toString.toLong - last_login_time);
              stack.pop();
            }
          }
          if(elem(0) == "3" )
          {
            stack.push(elem(0).toString);
          }
          if(elem(0) == "4")
          {
            // 假入下一个为7则要进栈（4-7）。
            if(tem_list(i+1)(0) == "7")
            {
              stack.push(elem(0).toString);
            }
            // 假入下一个为不7匹配（0-4）出栈
            if(stack.size != 0 && stack.top == "0")
            {
              count_second = count_second + (elem(1).toString.toLong - last_login_time);
              stack.pop();
            }
          }
          if(elem(0) == "7")
          {
            if(stack.size != 0 && (stack.top == "3" || stack.top == "4"))
            {
              stack.pop();
            }
          }
          i = i + 1;
        })
//        iterable.foreach(elem => {
//          // 登录
//          if(elem(0) == "0")
//          {
//            stack.push(elem(0).toString);
//            last_login_time = elem(1).toString.toLong;
//          }
//          // 登出、异常登出
//          if(elem(0) == "1" || elem(0) == "2")
//          {
//            if(stack.size != 0 && stack.top == "0")
//            {
//              count_second = count_second + (elem(1).toString.toLong - last_login_time);
//              stack.pop();
//            }
//          }
//          if(elem(0) == "3" )
//          {
//            stack.push(elem(0).toString);
//            last_login_time = elem(1).toString.toLong;
//          }
//          if(elem(0) == "4")
//          {
//            // 假入下一个为7则要出栈（4-7）。否则匹配0-4
//            stack.push(elem(0).toString);
//            last_login_time = elem(1).toString.toLong;
//          }
//          if(elem(0) == "7")
//          {
//            if(stack.size != 0 && (stack.top == "3" || stack.top == "4"))
//            {
//              stack.pop();
//            }
//          }
//          i = i + 1;
//        })
        map("online_time") = count_second;
      }
      map
    })
    online_doc_rdd.collect().foreach(println)
  }

}
