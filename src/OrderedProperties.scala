import scala.io.Source
import java.io.IOException
import java.io.FileNotFoundException

class OrderedProperties(fileName: String) {

  def getLinePairs(): Option[Iterator[List[String]]] = {

    val status = validateConfigFile()
    if (status) {
      try {
        val pairs = Source.fromFile(fileName).getLines()
          .filter(!_.isEmpty)
          .map(_.trim())
          .toList
          .grouped(2)

        Some(pairs)
      } catch {
        case e: FileNotFoundException =>
          println("Couldn't find that file.")
          None
        case e: IOException =>
          println("Got an IOException!")
          None
        case _: Throwable => None
      }

    } else None

  } //getLinePairs

  def validateConfigFile(): Boolean = {
    Source.fromFile(fileName).getLines()
      .filter(!_.isEmpty).length % 2 == 0
  } //validateConfigFile

} //OrderedProperties