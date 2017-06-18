import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

object Run {
  def main(args: Array[String]): Unit = {
    val hdfsConf = new Configuration()
    val hdfsFS = FileSystem.get(hdfsConf)

    val cs = hdfsFS.getContentSummary(new Path("/user/cloudera"))
    println(cs.getSpaceConsumed * 0.000000000931323 + " Gigs")
    println(cs.getLength)
    println(cs.getFileCount)
    val files = hdfsFS.listFiles(new Path("/user/cloudera"), true)
    while (files.hasNext()) {
      val file = files.next
      val blockSize = file.getBlockSize
      val blocksConsumed = (file.getLen / blockSize) + 1
      println(file.getPath + " " + file.getLen * 0.000000000931323 + " GB. Stored in " + blocksConsumed + " blocks in HDFS.")
      println(file.getBlockLocations)
    }
  }
}
