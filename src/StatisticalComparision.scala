import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.util.Properties

object StatisticalComparision {
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

    val sqlQuery = "select * from mysql_customer"
    val df = spark.sqlContext.sql(sqlQuery)
    df.describe().show()

    val targetDF = getDataFrame("mysql", "select * from customers").get
    targetDF.describe().show()

    val sourceStats = df.stat.freqItems(df.schema.fieldNames)
    val targetStats = targetDF.stat.freqItems(targetDF.schema.fieldNames)

    sourceStats.show()
    targetStats.show()

    println("Stats Match: " + sourceStats.except(targetStats).count)
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