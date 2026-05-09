package com.travelsky.dataplatform.main.ods;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.dim.TDimIdmappingError;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.UUID;

/**
 * @author kuangaihua
 * @date 2025/7/14 9:47
 */
public class DemoUpsertToDorisByTable {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        System.setProperty("java.security.krb5.conf", "src/main/resources/krb5.conf");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://cmss");
        conf.set("dfs.nameservices", "cmss");
        conf.set("dfs.ha.namenodes.cmss", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.cmss.nn1", "emr-master-001.novalocal:8020");
        conf.set("dfs.namenode.rpc-address.cmss.nn2", "emr-master-ha-001.novalocal:8020");
        conf.set("dfs.client.failover.proxy.provider.cmss",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("sys_ucv_test_for_mapreduce@BCHKDC", "src/main/resources/sys_ucv_test_for_mapreduce.keytab");
        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        System.out.println("Logged in as: " + currentUser.getUserName());
        String etlDate = args[0];
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        UUID uuid = UUID.randomUUID();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        long timestamp = System.currentTimeMillis();
        // register a table in the catalog
        tEnv.executeSql("CREATE TABLE T_DIM_IDMAPPING_ERROR (\n" +
                "    ERROR_TID VARCHAR(256) NOT NULL COMMENT '异常TID',\n" +
                "    SAME_STRONGID  VARCHAR(256) COMMENT '相同强ID',\n" +
                "    DIFF_STRONGID  VARCHAR(256) COMMENT '差异强ID',\n" +
                "    ETL_DATE DATE COMMENT '数据ETL日期',\n" +
                "    CREATE_TIME TIMESTAMP(6) COMMENT '创建时间',\n" +
                "    UPDATE_TIME TIMESTAMP(6) COMMENT '更新时间'" +
                ") WITH (\n" +
                "  'connector' = 'doris',\n"
                + "  'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "',\n"
                + "  'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "',\n"
                + "  'table.identifier' = '" + Constants.DIM_DB + ".T_DIM_IDMAPPING_ERROR',\n"
                + "'sink.label-prefix' = '" + timestamp+uuid + "',"
                + "  'sink.enable-delete' = 'false',"
                + "  'sink.properties.read_json_by_line' = 'true',"
                + "  'sink.properties.format' = 'json',"
                + "  'username' = '"+Constants.DIM_USER+"',\n"
                + "  'password' = '"+Constants.DIM_PWD+"'\n"
                + ")");
        String sql =
//                "insert into T_DIM_IDMAPPING_ERROR (ERROR_TID,SAME_STRONGID,ETL_DATE,CREATE_TIME,UPDATE_TIME) " +
                //字符串类型的时间
                //" SELECT '3', CAST('2025-07-11' AS DATE) ETL_DATE, TO_TIMESTAMP('2025-07-25 09:38:53.111', 'yyyy-MM-dd HH:mm:ss.SSS') AS UPDATE_TIME";
                //long 类型
        " SELECT '3' AS ERROR_TID,'2' SAME_STRONGID, CAST('2025-07-12' AS DATE) ETL_DATE,"+"TO_TIMESTAMP_LTZ("+timestamp+", 3) AS CREATE_TIME,"+"TO_TIMESTAMP_LTZ("+timestamp+", 3) AS UPDATE_TIME";
                //直接用系统时间
                //" SELECT '3', CAST('2025-07-11' AS DATE) ETL_DATE, CURRENT_TIMESTAMP(3)  AS UPDATE_TIME";

        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.print();

    }
}
