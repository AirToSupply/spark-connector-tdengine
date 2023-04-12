package org.apache.spark.sql.tdengine.example.write;

import com.taosdata.jdbc.SchemalessWriter;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
public class SchemalessWriteApplication {
    private static Connection getConnection() throws SQLException {
        Properties conf = new Properties();
        conf.setProperty(TSDBDriver.PROPERTY_KEY_HOST, "localhost");
        conf.setProperty(TSDBDriver.PROPERTY_KEY_PORT, "6030");
        conf.setProperty(TSDBDriver.PROPERTY_KEY_USER, "root");
        conf.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, "taosdata");
        conf.setProperty(TSDBDriver.PROPERTY_KEY_DBNAME, "test");
        Connection connection = DriverManager.getConnection("jdbc:TAOS://", conf);
        return connection;
    }

    private static String[] data() {
        String[] data = {
            "sensor,region=beijing pm25_aqi=11i,pm10_aqi=70i,no2_aqi=21i,temperature=-6i,pressure=999i,humidity=48i,wind=3i,weather=1i 1648432611249500",
            "sensor,region=beijing pm25_aqi=14i,pm10_aqi=71i,no2_aqi=31i,temperature=-1i,pressure=925i,humidity=46i,wind=4i,weather=2i 1648432611349500",
            "sensor,region=beijing pm25_aqi=15i,pm10_aqi=71i,no2_aqi=31i,temperature=-1i,pressure=925i,humidity=46i,wind=4i,weather=2i 1648432611549500",
        };
        return data;
    }

    public static void main(String[] args) throws SQLException {
        try (Connection conn = getConnection()) {
            SchemalessWriter writer = new SchemalessWriter(conn);
            writer.write(data(), SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);
        }
    }
}
