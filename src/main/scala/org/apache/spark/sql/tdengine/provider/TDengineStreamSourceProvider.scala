package org.apache.spark.sql.tdengine.provider

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.tdengine.convertor.TDengineRecordToRowConvertor
import org.apache.spark.sql.tdengine.source.TDengineSourceBaseOnSubscribe
import org.apache.spark.sql.types.StructType

/**
 * Based on topic subscribe to consumer continuous data for TSDB
 */
class TDengineStreamSourceProvider extends DataSourceRegister
  with StreamSourceProvider
  with Logging {

  override def shortName(): String = "tdengine"

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = (providerName,
    TDengineRecordToRowConvertor.rawDataSchema)

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = new TDengineSourceBaseOnSubscribe(sqlContext, metadataPath, schema,
    parameters: Map[String, String])
}
