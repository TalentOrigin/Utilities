

import scala.util.Try

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql.SparkSession
import java.io.BufferedReader
import java.io.InputStreamReader

object GzipFiles {
  val spark = SparkSession.builder()
    .appName("GZip files Testing")
    .master("local")
    .getOrCreate()
  def main(args: Array[String]): Unit = {
    val byteBuffer = 1024

    val rdd = spark.sparkContext.binaryFiles("sample_data/")
      .flatMap {
        case (fileName: String, pds: PortableDataStream) =>
          extractFiles(pds)
      }

    val headers = rdd.filter(_.startsWith("customer_identifier"))
    headers.foreach(println)

    val fileContents = rdd.filter(!_.startsWith("customer_identifier"))
    println("Header Count: " + headers.count)
    println("Contents Count: " + fileContents.count)
  }

  def extractFiles(pds: PortableDataStream): List[String] = {
    val gzipStream = new GzipCompressorInputStream(pds.open)
    val br = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"))
    Stream.continually(br.readLine).takeWhile(_ != null).toList
  }

}