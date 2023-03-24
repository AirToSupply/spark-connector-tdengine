package org.apache.spark.sql.tdengine.provider

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.tdengine.sink.TDengineSink

class TDengineStreamSinkProvider extends DataSourceRegister
  with StreamSinkProvider
  with Logging {

  override def shortName(): String = "tdengine"

  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = new TDengineSink(sqlContext, parameters, outputMode)
}
