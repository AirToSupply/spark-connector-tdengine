package org.apache.spark.sql.tdengine.example.sink

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.tdengine.provider.TDengineStreamSinkProvider

/**
 * Continuous writing to TDengine from `nc`
 *
 * You need to enter `nc lk 7777` on the terminal and it is started an interactive service to simulate
 * the input of raw data record like `line protocol` about to TDengine.
 *
 * The input data may be as follows:
 *
 *   sensor,region=beijing  pm25_aqi=11i,pm10_aqi=70i,no2_aqi=21i,temperature=-6i,pressure=999i,humidity=48i,wind=3i,weather=1i 1648432611249500
 *   sensor,region=beijing  pm25_aqi=14i,pm10_aqi=71i,no2_aqi=31i,temperature=-1i,pressure=925i,humidity=46i,wind=4i,weather=2i 1648432611349500
 *   sensor,region=beijing  pm25_aqi=15i,pm10_aqi=71i,no2_aqi=31i,temperature=-1i,pressure=925i,humidity=46i,wind=4i,weather=2i 1648432611549500
 *   sensor,region=shanghai pm25_aqi=21i,pm10_aqi=71i,no2_aqi=11i,temperature=-9i,pressure=999i,humidity=88i,wind=3i,weather=3i 1648432611249500
 *   sensor,region=shanghai pm25_aqi=34i,pm10_aqi=72i,no2_aqi=41i,temperature=-8i,pressure=925i,humidity=76i,wind=4i,weather=4i 1648432611449500
 *   sensor,region=shanghai pm25_aqi=45i,pm10_aqi=77i,no2_aqi=41i,temperature=-7i,pressure=925i,humidity=96i,wind=4i,weather=2i 1648432611949500
 */
object TDengineStreamSinkApplication {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("source-nc-sink-tdengine")
      .master("local[*]")
      .getOrCreate()

    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 7777)
      .load()
      .writeStream
      .format(classOf[TDengineStreamSinkProvider].getName)
      .outputMode("append")
      .option("host", "localhost")
      .option("port", "6030")
      .option("user", "root")
      .option("password", "taosdata")
      .option("dbname", "test")
      .option("write.mode", "schemaless")
      .option("schemaless.protocol.type", "LINE")
      .option("schemaless.timestamp.type", "MICRO_SECONDS")
      .option("checkpointLocation", "/tmp/source_nc_sink_tdengine_checkpoint")
      .start()
      .awaitTermination()
  }
}
