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

## Configuration options


| Parameter Name           | Description                                                                                                                                                                                                                                                                    | Default Value  |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------- |
| subscribe                | 【Require】The topic subscribed by the consumer.<br/>TDengine provides data subscription and consumption interfaces similar to Message Queuing products. For specific creation methods, please refer to [TDengine data subscription.](https://docs.taosdata.com/taos-sql/tmq/) | None           |
| bootstrap.servers        | 【Require】Subscription server connection address.                                                                                                                                                                                                                             | localhost:6030 |
| group.id                 | 【Require】Consumption group ID, sharing consumption progress with the same consumption group.                                                                                                                                                                                 | None           |
| deserializer.format      | Consumption data deserialization parsing format. Option:`json` and `csv`.                                                                                                                                                                                                      | json           |
| consumer.poll.timeout.ms | The timeout in milliseconds to poll data from TDengine.                                                                                                                                                                                                                        | 200            |
