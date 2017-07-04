import java.net.URI

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.compress.CompressionCodecFactory
import scala.collection.parallel.ForkJoinTaskSupport
import java.io.Closeable

object ParallelCopy extends App {

  val conf = new Configuration()
  val fs = FileSystem.get(conf)

  def copyUtil(sourcePath: Path, targetPath: Path) {
    val factory = new CompressionCodecFactory(conf)
    val codec = factory.getCodec(sourcePath)
    val targetFile = CompressionCodecFactory.removeSuffix(sourcePath.getName, codec.getDefaultExtension())

    try {
      val is = codec.createInputStream(fs.open(sourcePath))
      val os = codec.createOutputStream(fs.create(new Path(targetPath + "/" + targetFile)))
      try {
        IOUtils.copyBytes(is, os, conf)
      } finally {
        is.close()
        os.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }

  } //copyUtil

  val files = fs.listStatus(new Path("/user/cloudera/zipfiles"))
  var tempSeq = ArrayBuffer[Path]()
  files.map(file => tempSeq += file.getPath)
  val fileSeq = tempSeq.par
  fileSeq.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(4))

  val targetPath = new Path("/tmp/unzippedfiles/")

  var startTime = System.currentTimeMillis()
  fileSeq.map(filePath => copyUtil(filePath, targetPath))
  println("Time took to uncompress and copy 4 files of 36MB each 4-4: " + (System.currentTimeMillis() - startTime) / 1000.0 + " Seconds")

  startTime = System.currentTimeMillis()
  tempSeq.map(filePath => copyUtil(filePath, targetPath))
  println("Time took to uncompress and copy 4 files of 36MB each 1-1: " + (System.currentTimeMillis() - startTime) / 1000.0 + " Seconds")
}