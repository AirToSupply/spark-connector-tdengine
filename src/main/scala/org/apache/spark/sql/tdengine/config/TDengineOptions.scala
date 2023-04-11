package org.apache.spark.sql.tdengine.config

import com.taosdata.jdbc.enums.{SchemalessProtocolType, SchemalessTimestampType}
import org.apache.spark.sql.tdengine.serde.{CsvDeserializer, JsonDeserializer}

object TDengineOptions {

  /**
   * subscribe
   */
  val OPTION_SUBSCRIBE = "org/apache/spark/sql/tdengine/example/subscribe"

  /**
   * bootstrap servers
   */
  val OPTION_BOOTSTRAP_SERVERS = "bootstrap.servers"
  private val OPTION_BOOTSTRAP_SERVERS_VALUE = "localhost:6030"

  /**
   * group id
   */
  val OPTION_GROUP_ID = "group.id"

  /**
   * value deserializer format
   */
  val OPTION_DESERIALIZER_FORMAT = "deserializer.format"
  private val OPTION_DESERIALIZER_FORMAT_VALUE = "json"
  private val OPTION_DESERIALIZER_FORMAT_JSON = "json"
  private val OPTION_DESERIALIZER_FORMAT_CSV = "csv"

  /**
   * consumer poll timeout (ms)
   */
  val OPTION_CONSUMER_POLL_TIMEOUT_MS = "consumer.poll.timeout.ms"
  private val OPTION_CONSUMER_POLL_TIMEOUT_MS_VALUE = 200

  /**
   * server host
   */
  val OPTION_SERVER_HOST = "host"
  private val OPTION_SERVER_HOST_VALUE = "localhost"

  /**
   * server port
   */
  val OPTION_SERVER_PORT = "port"
  private val OPTION_SERVER_PORT_VALUE = "6030"

  /**
   * server username
   */
  val OPTION_SERVER_USER = "user"
  private val OPTION_SERVER_USER_VALUE = "root"

  /**
   * server password
   */
  val OPTION_SERVER_PASSWORD = "password"
  private val OPTION_SERVER_PASSWORD_VALUE = "taosdata"

  /**
   * server database
   */
  val OPTION_SERVER_DATABASE = "dbname"

  /**
   * batch size
   */
  val OPTION_BATCH_SIZE = "batchSize"
  private val OPTION_BATCH_SIZE_VALUE = "1000"

  /**
   * partition number
   */
  val OPTION_PARTITION_NUM = "numPartitions"

  /**
   * write mode
   */
  val OPTION_WRITE_MODE = "write.mode"
  private val OPTION_WRITE_MODE_VALUE = "schemaless"
  private val OPTION_WRITE_MODE_SCHEMALESS = "schemaless"
  private val OPTION_WRITE_MODE_JDBC = "jdbc"

  /**
   * Schemaless Protocol Type
   */
  val OPTION_SCHEMALESS_PROTOCOL_TYPE = "schemaless.protocol.type"
  private val OPTION_SCHEMALESS_PROTOCOL_TYPE_LINE = "LINE"
  private val OPTION_SCHEMALESS_PROTOCOL_TYPE_TELNET = "TELNET"
  private val OPTION_SCHEMALESS_PROTOCOL_TYPE_JSON = "JSON"

  /**
   * Schemaless Timestamp Type
   */
  val OPTION_SCHEMALESS_TIMESTAMP_TYPE = "schemaless.timestamp.type"
  private val OPTION_SCHEMALESS_TIMESTAMP_TYPE_HOURS = "HOURS"
  private val OPTION_SCHEMALESS_TIMESTAMP_TYPE_MINUTES = "MINUTES"
  private val OPTION_SCHEMALESS_TIMESTAMP_TYPE_SECONDS = "SECONDS"
  private val OPTION_SCHEMALESS_TIMESTAMP_TYPE_MILLI_SECONDS = "MILLI_SECONDS"
  private val OPTION_SCHEMALESS_TIMESTAMP_TYPE_MICRO_SECONDS = "MICRO_SECONDS"
  private val OPTION_SCHEMALESS_TIMESTAMP_TYPE_NANO_SECONDS = "NANO_SECONDS"

  def subscribe(options: Map[String, String]) = options.getOrElse(OPTION_SUBSCRIBE, {
    throw new IllegalArgumentException(s"Option '${OPTION_SUBSCRIBE}' must be require!")
  })

  def bootstrapServers(options: Map[String, String]) =
    options.getOrElse(OPTION_BOOTSTRAP_SERVERS, OPTION_BOOTSTRAP_SERVERS_VALUE)

  def groupId(options: Map[String, String]) = options.getOrElse(OPTION_GROUP_ID, {
    throw new IllegalArgumentException(s"Option '${OPTION_GROUP_ID}' must be require!")
  })

  def deserializerFormat(options: Map[String, String]) =
    options.getOrElse(OPTION_DESERIALIZER_FORMAT, OPTION_DESERIALIZER_FORMAT_VALUE) match {
      case OPTION_DESERIALIZER_FORMAT_JSON =>
        classOf[JsonDeserializer].getName
      case OPTION_DESERIALIZER_FORMAT_CSV =>
        classOf[CsvDeserializer].getName
      case _@format =>
        throw new IllegalArgumentException(s"Option '${OPTION_DESERIALIZER_FORMAT}' (${format}) is not supported!")
    }

  def consumerPollTimeoutMs(options: Map[String, String]) =
    options.getOrElse(OPTION_CONSUMER_POLL_TIMEOUT_MS, String.valueOf(OPTION_CONSUMER_POLL_TIMEOUT_MS_VALUE))

  def host(options: Map[String, String]) = options.getOrElse(OPTION_SERVER_HOST, OPTION_SERVER_HOST_VALUE)

  def port(options: Map[String, String]) = options.getOrElse(OPTION_SERVER_PORT, OPTION_SERVER_PORT_VALUE)

  def user(options: Map[String, String]) = options.getOrElse(OPTION_SERVER_USER, OPTION_SERVER_USER_VALUE)

  def password(options: Map[String, String]) = options.getOrElse(OPTION_SERVER_PASSWORD, OPTION_SERVER_PASSWORD_VALUE)

  def dbname(options: Map[String, String]) = options.getOrElse(OPTION_SERVER_DATABASE, {
    throw new IllegalArgumentException(s"Option '${OPTION_SERVER_DATABASE}' must be require!")
  })

  def batchSize(options: Map[String, String]) = options.getOrElse(OPTION_BATCH_SIZE, OPTION_BATCH_SIZE_VALUE).toInt

  def numPartitions(options: Map[String, String]) = options.get(OPTION_PARTITION_NUM).map(_.toInt)

  def writeMode(options: Map[String, String]) = options.getOrElse(OPTION_WRITE_MODE, OPTION_WRITE_MODE_VALUE)

  def schemalessProtocolType(options: Map[String, String]) =
    options.getOrElse(OPTION_SCHEMALESS_PROTOCOL_TYPE, OPTION_SCHEMALESS_PROTOCOL_TYPE_LINE) match {
      case OPTION_SCHEMALESS_PROTOCOL_TYPE_LINE =>
        SchemalessProtocolType.LINE
      case OPTION_SCHEMALESS_PROTOCOL_TYPE_TELNET =>
        SchemalessProtocolType.TELNET
      case OPTION_SCHEMALESS_PROTOCOL_TYPE_JSON =>
        SchemalessProtocolType.JSON
      case _@protocol =>
        throw new IllegalArgumentException(s"Option '${OPTION_SCHEMALESS_PROTOCOL_TYPE}' (${protocol}) is not supported!")
    }

  def schemalessTimestampType(options: Map[String, String]) =
    options.getOrElse(OPTION_SCHEMALESS_TIMESTAMP_TYPE, OPTION_SCHEMALESS_TIMESTAMP_TYPE_MICRO_SECONDS) match {
      case OPTION_SCHEMALESS_TIMESTAMP_TYPE_HOURS =>
        SchemalessTimestampType.HOURS
      case OPTION_SCHEMALESS_TIMESTAMP_TYPE_MINUTES =>
        SchemalessTimestampType.MINUTES
      case OPTION_SCHEMALESS_TIMESTAMP_TYPE_SECONDS =>
        SchemalessTimestampType.SECONDS
      case OPTION_SCHEMALESS_TIMESTAMP_TYPE_MILLI_SECONDS =>
        SchemalessTimestampType.MILLI_SECONDS
      case OPTION_SCHEMALESS_TIMESTAMP_TYPE_MICRO_SECONDS =>
        SchemalessTimestampType.MICRO_SECONDS
      case OPTION_SCHEMALESS_TIMESTAMP_TYPE_NANO_SECONDS =>
        SchemalessTimestampType.NANO_SECONDS
      case _@timestampType =>
        throw new IllegalArgumentException(s"Option '${OPTION_SCHEMALESS_TIMESTAMP_TYPE}' (${timestampType}) is not supported!")
    }
}
