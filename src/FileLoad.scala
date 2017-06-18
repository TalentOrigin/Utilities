import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object FileLoad {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()

    val spark = SparkSession.builder()
      .appName("Loading Table to DataFrame")
      .config(sparkConf)
      .enableHiveSupport()
      .master("local")
      .getOrCreate()

    val df = spark.sqlContext.read.parquet("/user/cloudera/parquet_sample")
    df.createTempView("tempTable")

    val tempDF = spark.sqlContext.sql("select customer_identifier, customer_type, tax_identifier, lob, legal_name from tempTable")
    tempDF.write.partitionBy("customer_type").mode(SaveMode.Overwrite)
      .saveAsTable("customer_partition")
  }
}