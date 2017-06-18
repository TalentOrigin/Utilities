import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.util.Properties

object JDBCLoad {
  val sparkConf = new SparkConf()

  val spark = SparkSession.builder()
    .appName("Loading Table to DataFrame")
    .config(sparkConf)
    .enableHiveSupport()
    .master("local")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val fileName = this.getClass.getClassLoader.getResource("jdbc_properties.conf").getPath
    val orderedProperties = new OrderedProperties(fileName)

    var queryPairs: Iterator[List[String]] = null
    try {
      queryPairs = orderedProperties.getLinePairs().get
    } catch {
      case e: NoSuchElementException =>
        e.printStackTrace()
        println("Both Source and Target SQL Queries should be provided in the config file")
    }

    val results = queryPairs.map {
      var status: String = null
      list =>
        val sourceLine = list(0).split("::")
        val targetLine = list(1).split("::")

        val sourceQueryName = sourceLine(0)
        val targetQueryName = targetLine(0)

        val sourceQuery = sourceLine(1).replaceAll("\"", "")
        val targetQuery = targetLine(1).replaceAll("\"", "")

        println(s"$sourceQueryName: $sourceQuery")
        println(s"$targetQueryName: $targetQuery")

        var sourceDF: org.apache.spark.sql.DataFrame = null
        var targetDF: org.apache.spark.sql.DataFrame = null

        try {
          sourceDF = getDataFrame("mysql", sourceQuery).get
          targetDF = getDataFrame("hive", targetQuery).get
        } catch {
          case e: NoSuchElementException =>
            //            e.printStackTrace()
            println("Source Type Not Supported.")
            System.exit(-1)
        }

        if (sourceDF.except(targetDF).count == 0) status = "SUCCESS"
        else status = "FAILURE"

        s"Comparision for $sourceQueryName and $targetQueryName: " + status
    } //map

    results.toArray.foreach(println)

  } //main

  def getDataFrame(source: String, query: String): Option[org.apache.spark.sql.DataFrame] = {
    source match {
      case "mysql" =>
        val url = "jdbc:mysql://192.168.1.40:3306/retail_db"
        val mySQLProperties = getMySqlProperties()
        Some(spark.sqlContext.read.jdbc(url, s"($query) as mysqlTable", mySQLProperties))

      case "hive" =>
        Some(spark.sqlContext.sql(query))
      case _ =>
        println(s"Framework current doesn't support $source source")
        None
    } //match
  } //getDataFrame

  def getMySqlProperties(): Properties = {
    val url = "jdbc:mysql://192.168.1.40:3306/retail_db"
    val mysqlProperties = new Properties()
    mysqlProperties.setProperty("user", "cloudera")
    mysqlProperties.setProperty("password", "cloudera")
    mysqlProperties.setProperty("driver", "com.mysql.jdbc.Driver")
    mysqlProperties
  } //getMySqlProperties
}