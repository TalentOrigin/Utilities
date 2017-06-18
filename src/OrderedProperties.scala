import scala.io.Source
import java.io.IOException
import java.io.FileNotFoundException

class OrderedProperties(fileName: String) {

  private def getQueryPairs(): Option[Iterator[List[String]]] = {
    val status = validateConfigFile()
    if (status) {
      try {
        val pairs = Source.fromFile(fileName).getLines()
          .filter(line => (!line.isEmpty) && (!line.startsWith("##")))
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

  def getValidQueryPairs(): Iterator[List[String]] = {
    val pairs = getQueryPairs().get
    pairs.filter {
      pairList =>
        val sourceLine = pairList(0).split("::")(1).replaceAll("\"", "")
        val targetLine = pairList(1).split("::")(1).replaceAll("\"", "")

        !(sourceLine.length < 1) && !(targetLine.length < 1)
    }
  }

  private def validateConfigFile(): Boolean = {
    Source.fromFile(fileName).getLines()
      .filter(line => (!line.isEmpty) && (!line.startsWith("##"))).length % 2 == 0
  } //validateConfigFile

} //OrderedProperties