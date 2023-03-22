package org.apache.spark.sql.tdengine.serde;

import com.taosdata.jdbc.TaosGlobalConfig;
import com.taosdata.jdbc.tmq.Deserializer;
import com.taosdata.jdbc.tmq.DeserializerException;
import com.taosdata.jdbc.tmq.TMQConstants;
import org.apache.commons.lang3.StringUtils;

import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
public class CsvDeserializer implements Deserializer<String> {

    private static final String COMMA = ",";

    @Override
    public void configure(Map<?, ?> configs) {
        Object encodingValue = configs.get(TMQConstants.VALUE_DESERIALIZER_ENCODING);
        if (encodingValue instanceof String)
            TaosGlobalConfig.setCharset(((String) encodingValue).trim());
    }

    @Override
    public String deserialize(ResultSet data) throws DeserializerException, SQLException {
        ArrayList<String> ployload = new ArrayList<>();
        ResultSetMetaData metaData = data.getMetaData();
        for (int columnIndex = 1; columnIndex <= metaData.getColumnCount(); columnIndex++) {
            String value = StringUtils.EMPTY;
            String columnTypeName = metaData.getColumnTypeName(columnIndex);

            if (!Objects.isNull(data.getObject(columnIndex))) {
                /**
                 * RowData Convert
                 * (1) ResultSet DOUBLE    => java.math.BigDecimal (PlainString)
                 * (2) ResultSet BINARY    => java.lang.String
                 * (3) ResultSet TIMESTAMP => java.lang.Long
                 */
                if ("DOUBLE".equals(columnTypeName)) {
                    int precision = metaData.getPrecision(columnIndex);
                    int scale = metaData.getScale(columnIndex);
                    value = data.getBigDecimal(columnIndex).setScale(scale, RoundingMode.DOWN).toPlainString();
                } else if ("BINARY".equals(columnTypeName)) {
                    value = data.getString(columnIndex);
                } else if ("TIMESTAMP".equals(columnTypeName)) {
                    value = String.valueOf(data.getLong(columnIndex));
                } else {
                    value = data.getObject(columnIndex).toString();
                }
            }

            ployload.add(value);
        }
        return String.join(COMMA, ployload);
    }

}
