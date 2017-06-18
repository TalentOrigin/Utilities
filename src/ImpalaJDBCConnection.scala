import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.util.Properties
import java.util.Calendar
import java.text.SimpleDateFormat

object ImpalaJDBCConnection {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()

    val spark = SparkSession.builder()
      .appName("Loading Table to DataFrame")
      .config(sparkConf)
      .enableHiveSupport()
      .master("local")
      .getOrCreate()

    val JDBCDriver = "com.cloudera.impala.jdbc41.Driver"
    val ConnectionURL = "jdbc:impala://192.168.1.40:21050/default;auth=noSasl"
    
    val properties = new Properties()
    
    val sqlQuery = "select * from customer where customer_type='ORG'"
    
    var now = Calendar.getInstance()
    var startTime = now.getTimeInMillis/1000.0
    
    val df = spark.sqlContext.read.jdbc(ConnectionURL, s"($sqlQuery) as impalaTable", properties)
    df.show()
    
    now = Calendar.getInstance()
    var endTime = now.getTimeInMillis/1000.0
    
    println("Time took for Impala execution: " + (endTime - startTime))
    
    now = Calendar.getInstance()
    startTime = now.getTimeInMillis/1000.0
    
    val hiveDF = spark.sqlContext.sql(sqlQuery)
    hiveDF.show()
    
    now = Calendar.getInstance
    endTime = now.getTimeInMillis/1000.0
    println("Time took for Hive execution: " + (endTime - startTime))

  }
}