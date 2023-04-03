package org.apache.spark.sql.tdengine.write

import com.taosdata.jdbc.TSDBDriver
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.tdengine.config.TDengineOptions
import org.apache.spark.sql.tdengine.util.TDengineJdbcUtils
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}

import java.sql.Connection
import java.util.Properties

object SchemalessWrite extends Logging {

  private val VALUE_ATTRIBUTE_NAME: String = "value"

  def write(
    sparkSession: SparkSession,
    data: DataFrame,
    parameters: Map[String, String]): Unit = {

    validateQuery(data.queryExecution.analyzed.output)

    val conf = new Properties
    conf.setProperty(TSDBDriver.PROPERTY_KEY_HOST, TDengineOptions.host(parameters))
    conf.setProperty(TSDBDriver.PROPERTY_KEY_PORT, TDengineOptions.port(parameters))
    conf.setProperty(TSDBDriver.PROPERTY_KEY_USER, TDengineOptions.user(parameters))
    conf.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TDengineOptions.password(parameters))
    conf.setProperty(TSDBDriver.PROPERTY_KEY_DBNAME, TDengineOptions.dbname(parameters))

    val getConnection: () => Connection = TDengineJdbcUtils.createConnectionFactory(conf)
    val batchSize = TDengineOptions.batchSize(parameters)
    val protocolType = TDengineOptions.schemalessProtocolType(parameters)
    val timestampType = TDengineOptions.schemalessTimestampType(parameters)

    val repartitionedDF = TDengineOptions.numPartitions(parameters) match {
      case Some(n) if n <= 0 =>
        throw new IllegalArgumentException(
          s"Invalid value `$n` for parameter `${TDengineOptions.OPTION_PARTITION_NUM}` " +
            s"in table writing via TDengine. The minimum value is 1.")
      case Some(n) if n < data.rdd.getNumPartitions => data.coalesce(n)
      case _ => data
    }

    repartitionedDF.queryExecution.toRdd.foreachPartition { iter =>
      val writeTask = new SchemalessWriteTask(
        TaskContext.get().partitionId(), getConnection, batchSize, protocolType, timestampType)
      writeTask.execute(iter)
    }
  }

  private def validateQuery(schema: Seq[Attribute]): Unit = {
    try {
      valueExpression(schema)
    } catch {
      case e: IllegalStateException => throw new AnalysisException(e.getMessage)
    }
  }

  private def valueExpression(schema: Seq[Attribute]): Expression = {
    expression(schema, VALUE_ATTRIBUTE_NAME, Seq(StringType, BinaryType)) {
      throw new IllegalStateException(s"Required attribute '${VALUE_ATTRIBUTE_NAME}' not found")
    }
  }

  private def expression(
    schema: Seq[Attribute],
    attrName: String,
    desired: Seq[DataType])(
    default: => Expression): Expression = {
    val expr = schema.find(_.name == attrName).getOrElse(default)
    if (!desired.exists(_.sameType(expr.dataType))) {
      throw new IllegalStateException(s"$attrName attribute unsupported type " +
        s"${expr.dataType.catalogString}. $attrName must be a(n) " +
        s"${desired.map(_.catalogString).mkString(" or ")}")
    }
    expr
  }
}
