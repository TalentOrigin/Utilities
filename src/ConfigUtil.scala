import java.io.File
import java.io.FileInputStream
import java.util.Map.Entry
import java.util.Properties

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigRenderOptions
import com.typesafe.config.ConfigValue
import scala.collection.mutable.ArrayBuffer

class ConfigUtil(fileName: String, tableName: String = "") {

  var config: Config = _
  var entrySet: java.util.Set[Entry[String, ConfigValue]] = _
  var keyList: ArrayBuffer[String] = _

  if (tableName.isEmpty) {
    println(s"loading properties from $fileName")
    config = ConfigFactory.parseFile(new File(fileName))
  } else {
    println(s"loading properties from $fileName.$tableName")
    config = ConfigFactory.parseFile(new File(fileName)).getConfig(tableName)
  }

  val is = new FileInputStream(fileName)

  val props = new Properties()
  props.load(is)

  def envOrElseConfig(name: String): String = {
    scala.util.Properties.envOrElse(
      name.toUpperCase.replaceAll("""\.""", "_"),
      config.getString(name))
  }

  def validateConfigFile(): String = {
    entrySet = config.entrySet()

    val iter = entrySet.iterator()
    while (iter.hasNext()) {

      val sourceItem = iter.next
      val targetItem = iter.next

      println(sourceItem)
      println(targetItem)
      //      keyList += sourceItem.getKey
      //      keyList += targetItem.getKey

      val sourceItemStatus = sourceItem.toString().contains("source_")
      val targetItemStatus = targetItem.toString().contains("target_")

      if (!(sourceItemStatus == targetItemStatus)) {
        keyList.clear()
        println("Provide source and target queries properly")
        return "invalid"
      } //if
    } //while
    "valid"
  } //validate

  def getKeyArray(): Array[String] = keyList.toArray
}