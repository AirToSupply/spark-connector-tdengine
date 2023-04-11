package org.apache.spark.sql.tdengine.write

import com.taosdata.jdbc.SchemalessWriter
import com.taosdata.jdbc.enums.{SchemalessProtocolType, SchemalessTimestampType}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import scala.collection.JavaConverters._

import java.sql.Connection

class SchemalessWriteTask(
  partitionId: Int,
  getConnection: () => Connection,
  batchSize: Int,
  protocolType: SchemalessProtocolType,
  timestampType: SchemalessTimestampType) extends Logging {

  def execute(iterator: Iterator[InternalRow]): Unit = {
    logInfo(s"SchemalessWriterTask execute by partition (${partitionId})")
    val connection = getConnection()
    val writer = new SchemalessWriter(connection)
    try {
      var rowCount = 0
      val records = scala.collection.mutable.ArrayBuffer[String]()
      while (iterator.hasNext) {
        val row = iterator.next()
        records.append(row.getString(0))
        rowCount += 1
        if (rowCount % batchSize == 0) {
          writer.write(records.toList.asJava, protocolType, timestampType)
          rowCount = 0
          records.clear()
        }
      }
      if (records.nonEmpty) {
        writer.write(records.toList.asJava, protocolType, timestampType)
        rowCount = 0
        records.clear()
      }
    } catch {
      case e: Exception =>
        logError(s"Writing data failed for partition [${partitionId}], cause by", e)
    } finally {
      try {
        if (null != connection) {
          connection.close()
        }
      } catch {
        case e: Exception =>
          logWarning(s"Writing data succeeded, but closing failed for partition [${partitionId}]", e)
      }
    }
  }
}

object SchemalessWriteTask {
  def apply(
    partitionId: Int,
    getConnection: () => Connection,
    batchSize: Int,
    protocolType: SchemalessProtocolType,
    timestampType: SchemalessTimestampType) =
    new SchemalessWriteTask(
      partitionId,
      getConnection,
      batchSize,
      protocolType,
      timestampType)
}
