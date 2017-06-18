import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import java.util.NoSuchElementException

object SparkTest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Spark Data Comparision")
    //
    //    val sc = new SparkContext(sparkConf)
    //    val hiveContext = new HiveContext(sc)

    val spark = SparkSession.builder()
      .appName("Loading Table to DataFrame")
      .config(sparkConf)
      .enableHiveSupport()
      .master("local")
      .getOrCreate()

    val fileName = this.getClass.getClassLoader.getResource("properties.conf").getPath
    //        val props = new PropertiesUtil(fileName)
    //    
    //        val propNames = props.getPropNames()
    //    
    //            val result = propNames.map {
    //              case (i, v) =>
    //                val sourceQuery = v.split(",")(0)
    //                val targetQuery = v.split(",")(1)
    //                println(s"$sourceQuery: " + props.getValue(sourceQuery))
    //                println(s"$targetQuery: " + props.getValue(targetQuery))
    //        
    //                val sourceDF = spark.sqlContext.sql(props.getValue(sourceQuery))
    //                val targetDF = spark.sqlContext.sql(props.getValue(targetQuery))
    //        
    //                s"Mismatch Count for $sourceQuery and $targetQuery: " + sourceDF.except(targetDF).count
    //            }
    //        
    //            result.foreach(println)
    //        val props = new PropertiesUtil(fileName)
    val props = new OrderedProperties(fileName)
    var queryPairs: Iterator[List[String]] = null
    try {
      val queryPairs = props.getLinePairs().get
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

        val sourceDF = spark.sqlContext.sql(sourceQuery)
        val targetDF = spark.sqlContext.sql(targetQuery)

        if (sourceDF.except(targetDF).count == 0) status = "SUCCESS"
        else status = "FAILURE"

        s"Mismatch Count for $sourceQueryName and $targetQueryName: " + status
    } //map

    results.toArray.foreach(println)

  } //main
}