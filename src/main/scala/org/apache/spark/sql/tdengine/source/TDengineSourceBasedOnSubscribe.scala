package org.apache.spark.sql.tdengine.source

import com.taosdata.jdbc.tmq.{TMQConstants, TaosConsumer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path, PathFilter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.{CheckpointFileManager, Offset, OffsetSeqLog, SerializedOffset, Source}
import org.apache.spark.sql.tdengine.config.TDengineOptions
import org.apache.spark.sql.tdengine.convertor.TDengineRecordToRowConvertor
import org.apache.spark.sql.types.StructType

import java.util.{Collections, Iterator, Objects, Properties}
import java.util.concurrent.locks.{Lock, ReentrantLock}
import TDengineSourceBasedOnSubscribe._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.tdengine.offset.LongOffset
import org.apache.spark.unsafe.types.UTF8String

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.control.NonFatal

class TDengineSourceBasedOnSubscribe(
  sqlContext: SQLContext,
  metadataPath: String,
  schema: Option[StructType],
  parameters: Map[String, String],
  maxBatchNumber: Long = Long.MaxValue,
  maxBatchSize: Long = Long.MaxValue,
  maxRetryNumber: Int = 3) extends Source with Logging {

  // Last batch offset file index
  private var lastOffset: Long = -1L

  // Current data file index to write messages.
  private var currentMessageDataFileOffset: Long = 0L

  // FileSystem instance for storing received messages.
  private var fs: FileSystem = _
  private var messageStoreOutputStream: FSDataOutputStream = _

  // total message number received for current batch.
  private var messageNumberForCurrentBatch: Int = 0

  // total message size received for
  private var messageSizeForCurrentBatch: Int = 0

  // The minimum number of batches that must be retained and made recoverable.
  private val minBatchesToRetain = sqlContext.sparkSession.sessionState.conf.minBatchesToRetain

  // The consecutive fail number, cannot exceed the `maxRetryNumber`
  private var consecutiveFailNum = 0

  private val lock: Lock = new ReentrantLock()

  // The TDengine consumer
  private var consumer: TaosConsumer[String] = _

  private val stopped = new AtomicBoolean(false)

  private var writeAheadLogThread: Thread = _

  private val hadoopConfig: Configuration = TDengineSourceBasedOnSubscribe.hadoopConfig match {
    case null =>
      logInfo("create a new configuration.")
      new Configuration()
    case _ =>
      logInfo("using setted hadoop configuration!")
      TDengineSourceBasedOnSubscribe.hadoopConfig
  }

  /**
   * checkpoint root path
   */
  private val rootCheckpointPath = {
    val path = new Path(metadataPath).getParent.getParent.toUri.toString
    logInfo(s"get rootCheckpointPath $path")
    path
  }

  /**
   * checkpoint path for WAL data
   */
  private val receivedDataPath = s"$rootCheckpointPath/data"

  /**
   * [Recovery] Lazily init latest offset from offset WAL log
   */
  private lazy val recoveredLatestOffset = {
    // the index of this source, parsing from metadata path
    val currentSourceIndex = {
      if (!metadataPath.isEmpty) {
        metadataPath.substring(metadataPath.lastIndexOf("/") + 1).toInt
      } else {
        -1
      }
    }
    if (currentSourceIndex >= 0) {
      val offsetLog = new OffsetSeqLog(
        sqlContext.sparkSession, new Path(rootCheckpointPath, "offsets").toUri.toString)

      // get the latest offset from WAL log
      offsetLog.getLatest() match {
        case Some((batchId, _)) =>
          logInfo(s"get latest batch $batchId")
          Some(batchId)
        case None =>
          logInfo("no offset avaliable in offset log")
          None
      }
    } else {
      logInfo("checkpoint path is not set")
      None
    }
  }

  initialize

  // Change data file if reach flow control threshold for one batch.
  // Not thread safe.
  private def startWriteNewDataFile(): Unit = {
    if (messageStoreOutputStream != null) {
      logInfo(s"Need to write a new data file, close current data file index $currentMessageDataFileOffset")
      messageStoreOutputStream.flush()
      messageStoreOutputStream.hsync()
      messageStoreOutputStream.close()
      messageStoreOutputStream = null
    }
    currentMessageDataFileOffset += 1
    messageSizeForCurrentBatch = 0
    messageNumberForCurrentBatch = 0
    messageStoreOutputStream = null
  }

  // not thread safe
  private def addReceivedMessageInfo(messageNum: Int, messageSize: Int): Unit = {
    messageSizeForCurrentBatch += messageSize
    messageNumberForCurrentBatch += messageNum
  }

  // not thread safe
  private def hasNewMessageForCurrentBatch(): Boolean = {
    currentMessageDataFileOffset > lastOffset + 1 || messageNumberForCurrentBatch > 0
  }

  private def withLock[T](body: => T): T = {
    lock.lock()
    try body
    finally lock.unlock()
  }

  private def initialize(): Unit = {

    /**
     * Recover lastOffset from WAL log
     */
    if (recoveredLatestOffset.nonEmpty) {
      lastOffset = recoveredLatestOffset.get
      logInfo(s"Recover lastOffset value ${lastOffset}")
    }

    /**
     * FileSystem instance for storing received messages.
     */
    fs = FileSystem.get(hadoopConfig)

    /**
     * Recover message data file offset from hdfs
     */
    val dataPath = new Path(receivedDataPath)
    if (fs.exists(dataPath)) {
      val fileManager = CheckpointFileManager.create(dataPath, hadoopConfig)
      val dataFileIndexs = fileManager.list(dataPath, new PathFilter {
        private def isBatchFile(path: Path) = {
          try {
            path.getName.toLong
            true
          } catch {
            case _: NumberFormatException => false
          }
        }

        override def accept(path: Path): Boolean = isBatchFile(path)
      }).map(_.getPath.getName.toLong)
      if (dataFileIndexs.nonEmpty) {
        currentMessageDataFileOffset = dataFileIndexs.max + 1
        assert(currentMessageDataFileOffset >= lastOffset + 1,
          s"Recovered invalid message data file offset $currentMessageDataFileOffset,"
            + s"do not match with lastOffset $lastOffset")
        logInfo(s"Recovered last message data file offset: ${currentMessageDataFileOffset - 1}, "
          + s"start from $currentMessageDataFileOffset")
      } else {
        logInfo("No old data file exist, start data file index from 0")
        currentMessageDataFileOffset = 0
      }
    } else {
      logInfo(s"Create data dir $receivedDataPath, start data file index from 0")
      fs.mkdirs(dataPath)
      currentMessageDataFileOffset = 0
    }

    /**
     * Create DTengine Consumer Instance and subscribe topic
     */
    consumer = createConsumer(parameters)
    consumer.subscribe(Collections.singletonList(TDengineOptions.subscribe(parameters)))
    logInfo(s"Connect complete ${TDengineOptions.bootstrapServers(parameters)} and subscribe topic for " +
      s"${TDengineOptions.subscribe(parameters)}")

    /**
     * Polling data continuously from DTengine server
     */
    writeAheadLogThread = new Thread("tdengine-consumer-pooling") {
      setDaemon(true)

      private def writeAheadLogArrived(record: String): Unit = withLock[Unit] {
        val recordSize = record.getBytes(StandardCharsets.UTF_8).size

        // check if have reached the max number or max size for current batch.
        if (messageNumberForCurrentBatch + 1 > maxBatchNumber || messageSizeForCurrentBatch + recordSize > maxBatchSize) {
          startWriteNewDataFile()
        }

        // write message content to data file
        if (messageStoreOutputStream == null) {
          val path = new Path(s"${receivedDataPath}/${currentMessageDataFileOffset}")
          if (fs.createNewFile(path)) {
            logInfo(s"Create new message data file ${path.toUri.toString} success!")
          } else {
            throw new IOException(
              s"${path.toUri.toString} already exist, make sure do use unique checkpoint path for each app.")
          }
          messageStoreOutputStream = fs.append(path)
        }

        messageStoreOutputStream.write(record.getBytes(StandardCharsets.UTF_8))
        messageStoreOutputStream.writeBytes("\n")
        addReceivedMessageInfo(1, recordSize)
        consecutiveFailNum = 0
      }

      def writeAheadLogFatal(cause: Throwable): Unit = withLock[Unit] {
        consecutiveFailNum += 1
        logError("Unexpected error in Thread [tdengine-consumer-pooling]", cause)
      }

      override def run(): Unit = {
        try {
          while (!stopped.get) {
            val records = consumer.poll(Duration.ofMillis(TDengineOptions.consumerPollTimeoutMs(parameters).toLong))
            val itr = records.iterator
            while (itr.hasNext) {
              val record = itr.next()
              if (!Objects.isNull(record)) {
                // WAL operation
                writeAheadLogArrived(record)
                // commit for TDengine Server
                consumer.commitSync()
              }
            }
          }
        } catch {
          case _: InterruptedException =>
          case NonFatal(e) => writeAheadLogFatal(e)
        }
      }
    }

    writeAheadLogThread.start()

  }

  override def schema: StructType = TDengineRecordToRowConvertor.rawDataSchema

  /**
   * Returns the maximum available offset for this source.
   */
  override def getOffset: Option[Offset] = withLock[Option[Offset]] {
    assert(consecutiveFailNum < maxRetryNumber,
      s"Write message data fail continuously for ${maxRetryNumber} times.")
    val result = if (!hasNewMessageForCurrentBatch()) {
      if (lastOffset == -1) {
        // first submit and no message has arrived.
        None
      } else {
        // no message has arrived for this batch.
        Some(LongOffset(lastOffset))
      }
    } else {
      // check if currently write the batch to be executed.
      if (currentMessageDataFileOffset == lastOffset + 1) {
        startWriteNewDataFile()
      }
      lastOffset += 1
      Some(LongOffset(lastOffset))
    }
    logInfo(s"getOffset result $result")
    result
  }

  /**
   * Returns the data that is between the offsets (`start`, `end`).
   * The batch return the data in file {checkpointPath}/data/{end}.
   * `Start` and `end` value have the relationship: `end value` = `start value` + 1,
   * if `start` is not None.
   */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    logInfo(s"getBatch with start = $start, end = $end")
    if (start.isDefined && start.get == end) {
      return sqlContext.internalCreateDataFrame(
        sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty"),
        TDengineRecordToRowConvertor.rawDataSchema,
        isStreaming = true)
    }


    withLock[Unit] {
      assert(consecutiveFailNum < maxRetryNumber,
        s"Write data fail continuously for ${maxRetryNumber} times.")
    }

    val endIndex = getOffsetValue(end)
    if (start.nonEmpty) {
      val startIndex = getOffsetValue(start.get)
      assert(startIndex + 1 == endIndex, s"start offset: ${startIndex} and end offset: ${endIndex} do not match")
    }

    logTrace(s"Create a data frame using hdfs file $receivedDataPath/$endIndex")

    val rdd = sqlContext.sparkContext.textFile(
      s"$receivedDataPath/$endIndex",
      sqlContext.sparkContext.defaultParallelism).mapPartitions { case itr =>
      itr.map {value => InternalRow(UTF8String.fromString(value))}
    }

    sqlContext.internalCreateDataFrame(
      rdd.setName("TDengine"),
      TDengineRecordToRowConvertor.rawDataSchema,
      true)
  }

  /**
   * Remove the data file for the offset.
   *
   * @param end the end of offset that all data has been committed.
   */
  override def commit(end: Offset): Unit = {
    val offsetValue = getOffsetValue(end)
    if (offsetValue >= minBatchesToRetain) {
      val deleteDataFileOffset = offsetValue - minBatchesToRetain
      try {
        fs.delete(new Path(s"$receivedDataPath/$deleteDataFileOffset"), false)
        logInfo(s"Delete committed offset data file $deleteDataFileOffset success!")
      } catch {
        case e: Exception =>
          logWarning(s"Delete committed offset data file $deleteDataFileOffset failed. ", e)
      }
    }
  }

  override def stop(): Unit = {
    logInfo("Stop TDengine source.")
    if (stopped.compareAndSet(false, true)) {
      writeAheadLogThread.interrupt()
    }
    consumer.unsubscribe()
    consumer.close()
    withLock[Unit] {
      if (messageStoreOutputStream != null) {
        messageStoreOutputStream.hflush()
        messageStoreOutputStream.hsync()
        messageStoreOutputStream.close()
        messageStoreOutputStream = null
      }
      fs.close()
    }
  }
}

object TDengineSourceBasedOnSubscribe {

  var hadoopConfig: Configuration = _

  private val OPTION_EXPERIMENTAL_SNAPSHOT_ENABLE = "experimental.snapshot.enable"

  private def createConsumer(options: Map[String, String]): TaosConsumer[String] = {
    val conf = new Properties
    conf.setProperty(TMQConstants.BOOTSTRAP_SERVERS, TDengineOptions.bootstrapServers(options))
    conf.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, java.lang.Boolean.FALSE.toString)
    conf.setProperty(TMQConstants.GROUP_ID, TDengineOptions.groupId(options))
    conf.setProperty(TMQConstants.AUTO_OFFSET_RESET, "earliest")
    conf.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, java.lang.Boolean.TRUE.toString)
    conf.setProperty(OPTION_EXPERIMENTAL_SNAPSHOT_ENABLE, java.lang.Boolean.TRUE.toString)
    conf.setProperty(TMQConstants.VALUE_DESERIALIZER, TDengineOptions.deserializerFormat(options))
    new TaosConsumer[String](conf)
  }

  private def getOffsetValue(offset: Offset): Long = {
    val offsetValue = offset match {
      case o: LongOffset =>
        o.offset
      case so: SerializedOffset =>
        so.json.toLong
    }
    offsetValue
  }

}
