package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author kuangaihua
 * @date 2025/6/27 15:34
 */
public class DemoGetOracleData {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 配置Oracle连接参数
        String driver = "oracle.jdbc.driver.OracleDriver";
        String url = "jdbc:oracle:thin:@//"+Constants.WXAP_IP+":"+Constants.WXAP_PORT+"/"+Constants.WXAP_DB;
        String username = Constants.WXAP_USER;
        String password = Constants.WXAP_PWD;
        RowTypeInfo rowType = new RowTypeInfo(
                Types.BIG_DEC,    // id字段
                Types.STRING  // order_no字段

        );
        // 创建JDBC输入格式
        JdbcInputFormat inputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername(driver)
                .setDBUrl(url)
                .setUsername(username)
                .setPassword(password)
                .setRowTypeInfo(rowType)
                .setQuery("select ID,ORDER_NO from scwxh5.CHECKIN_TRP_ORDER cto ")
                .finish();
        DataStreamSource<Row> input = env.createInput(inputFormat);
        input.print();
        env.execute();
    }
}
