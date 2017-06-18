import scala.io.Source

object LocalTesting {
  def getValidQueryPairs(pairs: Iterator[List[String]]): Iterator[List[String]] = {
    println("getValidationPairs")
    val test = pairs.filter {
      pairList =>
        val sourceLine = pairList(0).split("::")(1).replaceAll("\"", "")
        val targetLine = pairList(1).split("::")(1).replaceAll("\"", "")

        !(sourceLine.length < 1) && !(targetLine.length < 1)
    }
    test
  }

  def main(args: Array[String]): Unit = {
    val fileName = this.getClass.getClassLoader.getResource("jdbc_properties.conf").getPath
    val orderedProperties = new OrderedProperties(fileName)
  }
}