package org.apache.spark.sql.tdengine.util

import com.taosdata.jdbc.TSDBDriver
import org.apache.spark.internal.Logging

import java.sql.{Connection, DriverManager}
import java.util.Properties

object TDengineJdbcUtils extends Logging {

  private val URL_PREFIX = "jdbc:TAOS://";

  def createConnectionFactory(options: Properties): () => Connection = {
    () => {
      Class.forName(classOf[TSDBDriver].getName)
      val connection = DriverManager.getConnection(URL_PREFIX, options);
      require(connection != null, "The driver could not open a TDengine JDBC connection. Check the configuration!")
      connection
    }
  }
}
