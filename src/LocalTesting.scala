import scala.io.Source

object LocalTesting {
  def main(args: Array[String]): Unit = {
    val jdbcDF = JDBCLoad.getDataFrame("mysql", "select * from customers").get
    val jdbcSchema = jdbcDF.schema

    val columnValueCounts = jdbcDF.flatMap(r =>
      (0 until jdbcSchema.length).map { idx =>
        //((columnIdx, cellValue), count)
        ((idx, r.get(idx)), 1l)
      }).reduceByKey(_ + _)
  }
}