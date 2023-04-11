package org.apache.spark.sql.tdengine.example.source

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.tdengine.provider.TDengineStreamSourceProvider

import java.util.concurrent.TimeUnit

object TDengineStreamSourceApplication {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("source-tdengine-sink-console")
      .master("local[*]")
      .getOrCreate()

    spark.readStream
      .format(classOf[TDengineStreamSourceProvider].getName)
      .option("org/apache/spark/sql/tdengine/example/subscribe", "topic_sensor_data")
      .option("bootstrap.servers", "localhost:6030")
      .option("group.id", "group_topic_sensor_data")
      .load()
      .writeStream
      .format("console")
      .outputMode("append")
      .option("numRows", Int.MaxValue)
      .option("truncate", false)
      .option("checkpointLocation", "/tmp/source_tdengine_sink_console_checkpoint")
      .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
      .start()
      .awaitTermination()
  }
}
