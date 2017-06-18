import java.io.FileInputStream
import java.io.File
import java.util.Properties

class PropertiesUtil(fileName: String) {
  
  val is = new FileInputStream(new File(fileName))
  val properties = new Properties()
  properties.load(is)

  import scala.collection.JavaConverters._
  val propNames = properties.propertyNames().asScala.map(_.toString).toArray
    .grouped(2).zipWithIndex.map(t => (t._2, t._1.mkString(","))).toMap

  propNames.foreach(println)

  def getValue(propertyKey: String): String = properties.getProperty(propertyKey)

  def getPropNames(): Map[Int, String] = propNames
  
}