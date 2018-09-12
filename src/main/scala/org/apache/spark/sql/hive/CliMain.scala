package org.apache.spark.sql.hive

import org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver

/**
  * Created by passionke on 2017/12/14.
  * 但识码中趣 何劳键上音
  */
object CliMain {

  def startCliMain(args: Array[String]): Unit = {
    SparkSQLCLIDriver.main(args)
  }
}
