package org.apache.spark.sql.tdengine.serde;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.AfterFilter;
import com.alibaba.fastjson.serializer.SerializeFilter;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.taosdata.jdbc.TaosGlobalConfig;
import com.taosdata.jdbc.tmq.Deserializer;
import com.taosdata.jdbc.tmq.DeserializerException;
import com.taosdata.jdbc.tmq.TMQConstants;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;

public class JsonDeserializer implements Deserializer<String> {

    private AfterFilter afterFilter = null;

    @Override
    public void configure(Map<?, ?> configs) {
        Object encodingValue = configs.get(TMQConstants.VALUE_DESERIALIZER_ENCODING);
        if (encodingValue instanceof String) {
            TaosGlobalConfig.setCharset(((String) encodingValue).trim());
        }

        afterFilter = new AfterFilter() {
            @Override
            public void writeAfter(Object object) {
                Field[] fields = object.getClass().getDeclaredFields();
                for (Field field : fields) {
                    if (field.getType() == BigDecimal.class) {
                        field.setAccessible(true);
                        Object value = null;
                        try {
                            value = field.get(object);
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                        writeKeyValue(field.getName(), value);
                    }
                }
            }
        };
    }

    @Override
    public String deserialize(ResultSet data) throws DeserializerException, SQLException {
        JSONObject ployload = new JSONObject(true);
        ResultSetMetaData metaData = data.getMetaData();

        for (int columnIndex = 1; columnIndex <= metaData.getColumnCount(); columnIndex++) {
            String key = metaData.getColumnLabel(columnIndex);
            Object value = null;
            String columnTypeName = metaData.getColumnTypeName(columnIndex);

            if (!Objects.isNull(data.getObject(columnIndex))) {
                /**
                 * RowData Convert
                 * (1) ResultSet DOUBLE ==> java.math.BigDecimal
                 * (2) ResultSet BINARY ==> java.lang.String
                 */
                if ("DOUBLE".equals(columnTypeName)) {
                    int precision = metaData.getPrecision(columnIndex);
                    int scale = metaData.getScale(columnIndex);
                    value = data.getBigDecimal(columnIndex).setScale(scale, RoundingMode.DOWN);
                } else if ("BINARY".equals(columnTypeName)) {
                    value = data.getString(columnIndex);
                } else {
                    value = data.getObject(columnIndex);
                }
            }

            ployload.put(key, value);
        }

        return JSON.toJSONString(
            ployload,
            // SerializeFilter
            new SerializeFilter[] {afterFilter},
            // SerializerFeature ...
            SerializerFeature.WriteMapNullValue,
            SerializerFeature.WriteBigDecimalAsPlain);
    }

}
