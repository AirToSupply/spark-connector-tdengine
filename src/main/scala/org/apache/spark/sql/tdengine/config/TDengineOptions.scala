package org.apache.spark.sql.tdengine.config

import org.apache.spark.sql.tdengine.serde.{CsvDeserializer, JsonDeserializer}

object TDengineOptions {

  /**
   * subscribe
   */
  val OPTION_SUBSCRIBE = "subscribe"

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
}
