package ac.cn.saya.example
import java.io.{FileInputStream, InputStreamReader}
import java.util.Properties

/**
 * 读取配置文件工具类
 */
object PropertiesUtil {
  def load(propertiesName:String): Properties ={
    val prop=new Properties()
    val file_stream: FileInputStream = new FileInputStream(propertiesName)
    prop.load(new InputStreamReader(file_stream, "UTF-8"))
    prop
  }

}
