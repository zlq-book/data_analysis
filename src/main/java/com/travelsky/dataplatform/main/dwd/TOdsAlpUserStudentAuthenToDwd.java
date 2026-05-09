package com.travelsky.dataplatform.main.dwd;

import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.CertDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.CustomDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.CheckinSegFactModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author kuangaihua
 * @date 2025/8/4 15:10
 */
public class TOdsAlpUserStudentAuthenToDwd {
    static final Logger logger = LoggerFactory.getLogger(TOdsAlpUserStudentAuthenToDwd.class);
    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            logger.error("etl_date参数为空");
            System.exit(0);
        }
        String etlDate = args[0];
        logger.info("etl_date:" + etlDate);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointUtils.setCheckpoint(env, "TOdsWxapWxCheckinInsureToDwd");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //注册数据源表
        tEnv.executeSql(GetTableSql.getQueryTOdsAlpUserStudentAuthen());
        //查询sql
        String query = "SELECT SCHOOL_EXPIRE_DATE,CUSTOMER_ID FROM T_ODS_ALP_USER_STUDENT_AUTHEN \n" +
                "WHERE CUSTOMER_ID IS NOT NULL AND ETL_DATE='" + etlDate + "'";
        logger.info("查询sql：" + query);
        Table table = tEnv.sqlQuery(query);
        // 转换为 DataStream<Row>
        DataStream<CustomDimModel> source = tEnv.toChangelogStream(table).map(
                row -> {
                    //学生身份有效期
                    LocalDate schoolExpireDate = row.getFieldAs("SCHOOL_EXPIRE_DATE");
                    String studentExpiry = DateTimeUtils.localDateToString(schoolExpireDate);
                    //customerId
                    String customerId = row.getFieldAs("CUSTOMER_ID");
                    Tid tid = new Tid();
                    tid.setTid(customerId);
                    tid.setCrmCustomerId(customerId);
                    String tidStr = IdMapping.idMappingFunction(tid, "ALP");
                    //T_ID+CUSTOMER_ID的加密值作为主键
                    String systemKey=tidStr+customerId;
                    CustomDimModel customDimModel = new CustomDimModel();
                    customDimModel.setSystemKey(systemKey);
                    customDimModel.setStudentExpiry(studentExpiry);
                    String currentDateTime = DateTimeUtils.getCurrentDateTime();
                    customDimModel.setUpdateTime(currentDateTime);
                    customDimModel.setCreateTime(currentDateTime);
                    return customDimModel;
                }
        );
//        source.print();
        //  创建 Doris Sink 并写入
        DorisSink<CustomDimModel> dorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_CUSTOM_DIM");
        //数据写入doris
        source.sinkTo(dorisSink);
        env.execute("TOdsAlpUserStudentAuthenToDwd") ;
    }
}
