# Spark Connector TDengine Data Source

A library for writing and reading data from TDengine using Spark SQL Streaming (or Structured streaming).

## Linking

Install package By Maven

```shell
mvn clean install -Dmaven.test.skip=true
```

If you need to deploy private Nexus Repository:

```shell
mvn clean deploy -Dmaven.test.skip=true
```

Import POM to your project:

```xml
<dependency>
    <groupId>tech.odes</groupId>
    <artifactId>spark-connector-tdengine</artifactId>
    <version>{{site.SPARK_VERSION}}</version>
</dependency>
```

Unlike using `--jars`, using `--packages` ensures that this library and its dependencies will be added to the classpath.

The `--packages` argument can also be used with `bin/spark-submit`.

This library is only for Scala 2.12.x, so users should replace the proper Scala version in the commands listed above.

## Examples

SQL Stream can be created with data streams received through TDengine using:

SQL Stream may be also transferred into TDengine using:

## Configuration 


| Parameter Name            | Description                                                  | Default Value  | Read | Write |
| ------------------------- | ------------------------------------------------------------ | -------------- | ---- | ----- |
| subscribe                 | 【Require】The topic subscribed by the consumer.<br/>TDengine provides data subscription and consumption interfaces similar to Message Queuing products. For specific creation methods, please refer to [TDengine data subscription.](https://docs.taosdata.com/taos-sql/tmq/) | None           | ✅    |       |
| bootstrap.servers         | 【Require】Subscription server connection address.           | localhost:6030 | ✅    |       |
| group.id                  | 【Require】Consumption group ID, sharing consumption progress with the same consumption group. | None           | ✅    |       |
| deserializer.format       | Consumption data deserialization parsing format. Option:`json` and `csv`. | json           | ✅    |       |
| consumer.poll.timeout.ms  | The timeout in milliseconds to poll data from TDengine.      | 200            | ✅    |       |
| host                      | 【Require】TDengine Server host                              | localhost      |      | ✅     |
| port                      | 【Require】TDengine Server port                              | 6030           |      | ✅     |
| user                      | 【Require】TDengine Server username                          | root           |      | ✅     |
| password                  | 【Require】TDengine Server password                          | taosdata       |      | ✅     |
| dbname                    | 【Require】TDengine Server database                          | None           |      | ✅     |
| batchSize                 | Specify how many pieces of data to write as a batch.         | 1000           |      | ✅     |
| numPartitions             | Number of partitions.                                        | None           |      | ✅     |
| write.mode                | Data write mode. Option:`schemaless` and `jdbc`.             | schemaless     |      | ✅     |
| schemaless.protocol.type  | When `write.mode` is `schemaless`, it is represents the protocol used for writing <br/>When protocol is `LINE`, it is compatible with the InfluxDB line protocol.<br/>When protocol is `JSON`, it is compatible with the OpenTSDB JSON.<br/>When protocol is `TELNET`, it is compatible with the OpenTSDB Telnet. | LINE           |      | ✅     |
| schemaless.timestamp.type | When `write.mode` is `schemaless`, it is represents the time accuracy used for writing. <br/>Option: `HOURS`, `MINUTES`, `SECONDS`, `MILLI_SECONDS`, `MICRO_SECONDS`, and `NANO_SECONDS`. | MICRO_SECONDS  |      | ✅     |

### 
