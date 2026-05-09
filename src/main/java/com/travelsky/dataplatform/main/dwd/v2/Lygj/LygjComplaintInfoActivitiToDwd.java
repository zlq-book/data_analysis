package com.travelsky.dataplatform.main.dwd.v2.Lygj;

import com.travelsky.dataplatform.constans.LygjCreateToSql;
import com.travelsky.dataplatform.udf.SM4DecryptUDF;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.transform.lygj.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 *
 */
public class LygjComplaintInfoActivitiToDwd {

    //private static final Logger logger = LoggerFactory.getLogger(LygjComplaintInfoActivitiToDwd.class);

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointUtils.setCheckpoint(env, "LygjComplaintInfoActivitiToDwd");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1. 创建Doris目标表
        tEnv.executeSql(LygjCreateToSql.COMPLAINT_INFO_ACTIVITI);
        // 2. 从Doris表中读取数据
        String query = "SELECT \n" +
                "ETL_DATE,\n" +
                "COMPLAINT_SERIAL_NUM,\n" +
                "TICKET_CHANNEL,\n" +
                "CLOSE_ADDITIONAL_CONTENT,\n" +
                "CLOSE_ADDITIONAL_DATE,\n" +
                "CLOSE_ADDITIONAL_OPERATOR,\n" +
                "CLOSE_ADDITIONAL_FILE,\n" +
                "COMMENT_SUMMARY,\n" +
                "IS_CONVERT_MEDIATION,\n" +
                "BEFORE_COMPLAINT_ID,\n" +
                "MEDIATE_FALG,\n" +
                "NOT_MEDIATE_REASON,\n" +
                "MEDIATION_CLAIM,\n" +
                "NEW_COMPLAINT_INFO,\n" +
                "ATTACH_RECOMMUNICATE,\n" +
                "ID,\n" +
                "COMPLAINT_TITLE,\n" +
                "PAX_NAME,\n" +
                "IS_GROUP,\n" +
                "SEX,\n" +
                "TK_LUGGAGE,\n" +
                "CARD_NUM,\n" +
                "CARD_TYPE,\n" +
                "PSG_TYPE,\n" +
                "COMP_DUTY_FFP,\n" +
                "CABIN_CLASS,\n" +
                "PHONENUM,\n" +
                "ADDRESS,\n" +
                "EMAIL,\n" +
                "MEMO,\n" +
                "FEED_NAME,\n" +
                "FEED_PHONE,\n" +
                "FLIGHT_NUM,\n" +
                "FLIGHT_DATE,\n" +
                "DEP_CITY,\n" +
                "ARR_CITY,\n" +
                "COMPLAINT_WAY,\n" +
                "COMPLAINT_ORI,\n" +
                "SUGGEST_TYPE,\n" +
                "SERVICE_TYPE,\n" +
                "SERVICE_TYPE_DETAIL,\n" +
                "COMPLAINT_PRIORITY,\n" +
                "COMPLAINT_CONTENT,\n" +
                "COMPLAINT_SUPPLEMENT_CONTENT,\n" +
                "PSG_REQ,\n" +
                "SERVICES_REMIND,\n" +
                "DEAL_DEPT,\n" +
                "ASSIT_DEPT,\n" +
                "COMPLAINT_DATE,\n" +
                "COMPLAINT_STATE,\n" +
                "ACCEPT_PEOPLE,\n" +
                "ACCEPT_DEPT,\n" +
                "RECALL_TIME,\n" +
                "PROCESS_ID,\n" +
                "ATTACH_COMPLAINT,\n" +
                "CLOSE_CONTENT,\n" +
                "CLOSE_DATE,\n" +
                "CLOSE_DAYS,\n" +
                "CLOSE_OP,\n" +
                "VALID,\n" +
                "COMPLAINT_RESULT,\n" +
                "ARCHIVE_VALID,\n" +
                "FIRST_DUTY_DEPT,\n" +
                "FIRST_DUTY_DEPT_SCORE,\n" +
                "INVOLVED_PERSON_ACCT,\n" +
                "INVOLVED_PERSONNEL_NAME,\n" +
                "UPDATE_PERSON,\n" +
                "RESULT_ATTACH,\n" +
                "UPDATE_DATE " +
                "FROM T_ODS_LYGJ_COMPLAINT_INFO_ACTIVITI " +
                "WHERE ETL_DATE='" + etlDate + "'";
        //logger.info("查询sql：" + query);
        Table joinedTable = tEnv.sqlQuery(query);
        // 转换为 DataStream<Row>
        DataStream<Row> sourceStream = tEnv.toDataStream(joinedTable);
        LygjCIAToDwdCustomerFeedbackFacts.result(sourceStream);
        //启动任务
        env.execute("ExtractLygjDpiToDwd");


    }
}
