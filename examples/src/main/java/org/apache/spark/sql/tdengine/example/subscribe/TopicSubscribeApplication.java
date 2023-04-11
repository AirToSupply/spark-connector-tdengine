package org.apache.spark.sql.tdengine.example.subscribe;

import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.TMQConstants;
import com.taosdata.jdbc.tmq.TaosConsumer;
import org.apache.spark.sql.tdengine.serde.JsonDeserializer;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TopicSubscribeApplication {

    private static final String BOOTSTRAP_SERVERS = "localhost:6030";

    private static final String ENABLE_AUTO_COMMIT = "false";

    private static final String GROUP_ID = "group_topic_sensor_data";

    // earliest / latest / none
    private static final String AUTO_OFFSET_RESET = "earliest";

    private static final String TOPIC = "topic_sensor_data";

    private static final int POLL_TIMEOUT = 100;

    public static void main(String[] args) throws SQLException {
        Properties properties = new Properties();
        //
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, BOOTSTRAP_SERVERS);
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, ENABLE_AUTO_COMMIT);
        //
        properties.setProperty(TMQConstants.GROUP_ID, GROUP_ID);
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, AUTO_OFFSET_RESET);
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty("experimental.snapshot.enable", "true");
        //
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, JsonDeserializer.class.getName());

        TaosConsumer consumer = new TaosConsumer(properties);
        // subscribe
        consumer.subscribe(Collections.singletonList(TOPIC));

        while (true) {
            ConsumerRecords<String> sensors = consumer.poll(Duration.ofMillis(POLL_TIMEOUT));
            for (String sensor : sensors) {
                System.out.println("[data] " + sensor);
                consumer.commitSync();
            }
        }
    }

}
