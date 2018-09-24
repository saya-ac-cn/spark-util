package algorithm

import scala.collection.mutable.Stack

object brackets {

  /**
    * 栈
    * 先进后出 first in last out
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val list = List((0,1),(4,4),(7,8),(1,11),(4,13),(7,15),(0,18),(4,19));
    val stack = new Stack[Int];//初始化栈
    var last_login_time = 0;//最后一次登录时间
    var count_second = 0;//统计时长
    // 开始遍历
    for(elem <- list){
      /**
        * 只有相关数据(0,4)才会进栈。1,2在栈顶为0时出栈，4在栈顶为0时出
        */
      // 登录
      if(elem._1 == 0)
      {
        println("进栈:"+elem._1);
        stack.push(elem._1);
        last_login_time = elem._2
        println("栈实时统计");
        println(stack);
      }
      // 登出、异常登出
      if(elem._1 == 1 || elem._1 == 2)
      {
        println("准备出栈......");
        if(stack.size != 0 && stack.top == 0)
        {
          println("当前栈顶的数据满足弹出条件......");
          count_second = count_second + (elem._2 - last_login_time);
          stack.pop();
          println("出栈完成......");
          println("栈实时统计");
          println(stack)
        }
        else
        {
          println("出栈失败......");
          println("栈实时统计");
          println(stack)
        }
      }
      if(elem._1 == 4)
        {
          // 判断 下一个是否为7  栈顶是否为登录：是-》长时间未操作 被迫离线
          if(stack.top == 0)
            {
              println("当前栈顶的数据满足弹出条件......");
              count_second = count_second + (elem._2 - last_login_time);
              stack.pop();
              println("出栈完成......");
              println("栈实时统计");
              println(stack)
            }
          else
          {
            println("进栈:"+elem._1);
            stack.push(elem._1);
            last_login_time = elem._2
            println("栈实时统计");
            println(stack);
          }
        }
          if(elem._1 == 7)
            {
              println("准备出栈......");
              if(stack.size !=0 && stack.top == 4)
                {
                  println("当前栈顶的数据满足弹出条件......");
                  count_second = count_second + (elem._2 - last_login_time);
                  stack.pop();
                  println("2号：出栈完成......");
                  println("2号：栈实时统计");
                  println(stack)
                }
              else
                {
                  println("出栈失败......");
                  println("栈实时统计");
                  println(stack)
                }
            }
    }

    println("统计时长为：" + count_second);

  }

}