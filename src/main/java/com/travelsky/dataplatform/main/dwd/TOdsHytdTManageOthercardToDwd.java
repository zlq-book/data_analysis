package com.travelsky.dataplatform.main.dwd;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.transform.hytd.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author kuangaihua
 * @date 2025/8/4 15:10
 */
public class TOdsHytdTManageOthercardToDwd {
    static final Logger logger = LoggerFactory.getLogger(TOdsHytdTManageOthercardToDwd.class);

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            logger.error("etl_date参数为空");
            System.exit(0);
        }
        String etlDate = args[0];
        logger.info("etl_date:" + etlDate);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointUtils.setCheckpoint(env, "TOdsHytdTManageOthercardToDwd");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //注册数据源表
        tEnv.executeSql(GetTableSql.getQueryTOdsHytdTManageOthercard());
        //查询sql
        String query = "SELECT \n" +
                "ETL_DATE\n" +
                ",ID\n" +
                ",CRM_CARDNO\n" +
                ",CARD_NO\n" +
                ",NAME\n" +
                ",NAME_EN\n" +
                ",CERT_TYPE\n" +
                ",CERT_NO\n" +
                ",END_TIME\n" +
                " FROM T_ODS_HYTD_T_MANAGE_OTHERCARD WHERE ETL_DATE='" + etlDate + "'";
        logger.info("查询sql：" + query);
        Table table = tEnv.sqlQuery(query);
        // 转换为 DataStream<Map<String, String>>，生成tid并做标准化
        DataStream<JSONObject> source = tEnv.toChangelogStream(table).map(new MapFunction<Row, JSONObject>() {
            @Override
            public JSONObject map(Row row) throws Exception {
                String id=row.getFieldAs("ID");
                //凤凰知音卡号
                String crmCardno=row.getFieldAs("CRM_CARDNO");
                //援疆卡卡号
                String cardNo=row.getFieldAs("CARD_NO");
                //姓名
                String name=row.getFieldAs("NAME");
                //英文姓名
                String nameEn=row.getFieldAs("NAME_EN");
                //中文姓名标准化
                name=NormalizationUtils.standardize(FieldType.CN_NAME, name,DataSource.MEMBER_WORLD);
                //英文名标准化
                nameEn=NormalizationUtils.standardize(FieldType.EN_NAME, nameEn,DataSource.MEMBER_WORLD);
                //证件类型
                String certType=row.getFieldAs("CERT_TYPE");
                //证件类型标准化
                certType=NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, certType,DataSource.MEMBER_WORLD);
                //证件号
                String certNo=row.getFieldAs("CERT_NO");
                logger.info("标准化前证件号:certNo"+certNo);
                //证件号标准化
                certNo=NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, certNo,DataSource.MEMBER_WORLD);
                logger.info("标准化后证件号:certNo"+certNo);
                //有效截止日期
                String endTime=DateTimeUtils.localDateTimeToString(row.getFieldAs("END_TIME"));
                JSONObject map = new JSONObject();
                map.put("id", id);
                map.put("crmCardno", crmCardno);
                map.put("cardNo", cardNo);
                map.put("name", name);
                map.put("nameEn", nameEn);
                map.put("certType", certType);
                map.put("certNo", certNo);
                map.put("endTime", endTime);
                Tid tid = new Tid();
                tid.setTid(certNo);
                Map<String, String> certification = new HashMap<>();
                certification.put(certType, certNo);
                tid.setFrequentTravelerCardno(cardNo);
                tid.setCertification(certification);
                String tidStr = IdMapping.idMappingFunction(tid, "HYTD");
                map.put("TID", tidStr);
                return map;
            }
        });
        //常客-援疆卡发放业务事实表
        TManageOthercardToYuanxjCardFactTrans.result(source);
        //写常旅客信息维表
        TManageOthercardToFfpDimTrans.result(source);
        env.execute("TOdsHytdTManageOthercardToDwd");
    }
}
