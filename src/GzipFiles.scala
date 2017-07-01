

import java.io.BufferedReader
import java.io.InputStreamReader

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object GzipFiles {

  val spark = SparkSession.builder()
    .appName("GZip files Testing")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val byteBuffer = 1024

    val rdd = spark.sparkContext.binaryFiles("/user/cloudera/bank_customer/")
      .flatMap {
        case (fileName: String, pds: PortableDataStream) =>
          extractFiles(pds)
      }

    val hiveSchema = spark.sqlContext.table("bank_customer").schema
    val colNames = hiveSchema.fieldNames

    val stringSchema = StructType(hiveSchema.map {
      field =>
        StructField(field.name, StringType, true)
    })

    stringSchema.printTreeString()

    val rowRDD = rdd.filter(!_.startsWith("customer_identifier")).map {
      line =>
        val lineArray = line.split(",")
        val lineRDD = generateRow(lineArray)
        lineRDD
    } //map

    val tempDF = spark.sqlContext.createDataFrame(rowRDD, stringSchema)
    val sourceDF = applySchema(tempDF, hiveSchema)
    sourceDF.printSchema()
    sourceDF.show()

    val targetDF = spark.sqlContext.sql("select * from bank_customer")

    println("Check Status: " + sourceDF.except(targetDF).count)
  }

  def applySchema(df: org.apache.spark.sql.DataFrame, schema: StructType): org.apache.spark.sql.DataFrame = {
    var tempDF = df
    schema.foreach {
      field =>
        tempDF = tempDF.withColumn(field.name, tempDF(field.name).cast(field.dataType))
    } //foreach
    tempDF
  }

  def extractFiles(pds: PortableDataStream): List[String] = {
    val gzipStream = new GzipCompressorInputStream(pds.open)
    val br = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"))
    Stream.continually(br.readLine).takeWhile(_ != null).toList
  }

  def generateRow(lineArray: Array[String]): Row = Row.fromSeq(lineArray)

}