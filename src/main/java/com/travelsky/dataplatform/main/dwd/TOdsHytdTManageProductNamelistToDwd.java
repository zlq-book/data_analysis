package com.travelsky.dataplatform.main.dwd;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.ActSignupFactModal;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.MemberRegisterFactModel;
import com.travelsky.trp.usercenter.data.analysis.transform.hytd.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * @author kuangaihua
 * @date 2025/8/4 15:10
 */
public class TOdsHytdTManageProductNamelistToDwd {
    static final Logger logger = LoggerFactory.getLogger(TOdsHytdTManageProductNamelistToDwd.class);
    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            logger.error("etl_date参数为空");
            System.exit(0);
        }
        String etlDate = args[0];
        logger.info("etl_date:" + etlDate);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointUtils.setCheckpoint(env, "TOdsHytdTManageProductNamelistToDwd");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //注册数据源表
        tEnv.executeSql(GetTableSql.getQueryTOdsHytdTManageProductNamelist());
        tEnv.executeSql(GetTableSql.getQueryTOdsHytdTManageProduct());
        //查询sql
        String query = "SELECT T1.ID\n" +
                ",T1.PRODUCT_ID\n" +
                ",T1.CREATED_TIME\n" +
                ",T1.MODIFIED_TIME\n" +
                ",T1.MEMBER_CARD\n" +
                ",T1.CRED_CODE\n" +
                ",T1.MOBILE_NUMBER\n" +
                ",T1.LEVEL_NAME\n" +
                ",T1.CN_LAST_NAME\n" +
                ",T1.CN_FIRST_NAME\n" +
                ",T1.LAST_NAME\n" +
                ",T1.FIRST_NAME\n" +
                ",T1.DATEOF_BIRTH\n" +
                ",T1.PROMO_CODE\n" +
                ",T1.CHANNEL_ID\n" +
                ",T1.CHANNEL_NAME\n" +
                ",T2.NAME"+
                " FROM T_ODS_HYTD_T_MANAGE_PRODUCT_NAMELIST T1 " +
                " LEFT JOIN T_ODS_HYTD_T_MANAGE_PRODUCT T2 ON T1.PRODUCT_ID=T2.ID\n" +
                " WHERE T1.ETL_DATE='" + etlDate + "'";
        logger.info("查询sql：" + query);
        Table table = tEnv.sqlQuery(query);
        // 转换为 DataStream<Map<String, String>>，生成tid并做标准化
        DataStream<ActSignupFactModal> source = tEnv.toChangelogStream(table).map(new MapFunction<Row, ActSignupFactModal>() {
            @Override
            public ActSignupFactModal map(Row row) throws Exception {
                //ID
                String id=row.getFieldAs("ID");
                //活动名称
                String productId=row.getFieldAs("PRODUCT_ID");
                //报名时间
                String createdTime=DateTimeUtils.localDateTimeToString(row.getFieldAs("CREATED_TIME"));
                //报名日期
                String createdDate=DateTimeUtils.localDateTimeToDateString(row.getFieldAs("CREATED_TIME"));
                //修改时间
                String modifiedTime=DateTimeUtils.localDateTimeToString(row.getFieldAs("MODIFIED_TIME"));
                //会员卡号
                String memberNumber=row.getFieldAs("MEMBER_CARD");
                //常客卡号标准化
                memberNumber=NormalizationUtils.standardize(FieldType.FREQUENT_FLYER_NUM, memberNumber,DataSource.MEMBER_WORLD);
                //证件号
                String credCode=row.getFieldAs("CRED_CODE");
                logger.info("标准化前证件号:credCode"+credCode);
                //证件号标准化
                credCode=NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, credCode,DataSource.MEMBER_WORLD);
                logger.info("标准化后证件号:credCode"+credCode);
                //手机号
                String mobileNumber = row.getFieldAs("MOBILE_NUMBER");
                System.out.println(("标准化前手机号:mobileNumber"+mobileNumber));
                //电话号码标准化
                mobileNumber = NormalizationUtils.standardize(FieldType.MOBILE_NO, mobileNumber,DataSource.MEMBER_WORLD);
                System.out.println(("标准化后手机号:mobileNumber"+mobileNumber));
                //会员级别
                String levelName = row.getFieldAs("LEVEL_NAME");
                //英文名
                String firstName=row.getFieldAs("FIRST_NAME");
                //英文姓
                String lastName=row.getFieldAs("LAST_NAME");
                //中文名
                String cnFirstName = row.getFieldAs("CN_FIRST_NAME");
                //中文姓
                String cnLastName = row.getFieldAs("CN_LAST_NAME");
                //生日
                String dateofBirth=null;
                if (row.getFieldAs("DATEOF_BIRTH") !=null){
                    dateofBirth = DateTimeUtils.localDateTimeToDateString(row.getFieldAs("DATEOF_BIRTH"));
                }
                //促销代码
                String promoCode = row.getFieldAs("PROMO_CODE");
                //报名渠道ID
                String channelId = row.getFieldAs("CHANNEL_ID");
                //渠道名称
                String channelName = row.getFieldAs("CHANNEL_NAME");

                Tid tid = new Tid();
                //修改为成证件号为tid，常客卡号为强ID
                //tid.setTid(credCode);
                tid.setTid(memberNumber);
                tid.setFrequentTravelerCardno(memberNumber);
                String tidStr = IdMapping.idMappingFunction(tid, "HYTD");
                ActSignupFactModal actSignupFactModal = new ActSignupFactModal();
                actSignupFactModal.setPkId(productId + memberNumber);
                actSignupFactModal.setFkActionDate(createdDate);
                actSignupFactModal.setProductId(productId);
                if (StringUtils.isNotBlank(productId)) {
                    actSignupFactModal.setProductCnName(row.getFieldAs("NAME"));
                }
                actSignupFactModal.setFkSignupTime(createdTime);
                actSignupFactModal.setModifiedTime(modifiedTime);
                actSignupFactModal.setMemberTid(tidStr);
                actSignupFactModal.setMemberCard(memberNumber);
                actSignupFactModal.setCertNumber(credCode);
                actSignupFactModal.setMobileNumber(mobileNumber);
                actSignupFactModal.setMemberLevel(levelName);
                actSignupFactModal.setEnFirstName(firstName);
                actSignupFactModal.setEnLastName(lastName);
                actSignupFactModal.setCnFirstName(cnFirstName);
                actSignupFactModal.setCnLastName(cnLastName);
                actSignupFactModal.setBirthday(dateofBirth);
                actSignupFactModal.setPromoCode(promoCode);
                actSignupFactModal.setChannelId(channelId);
                actSignupFactModal.setChannelName(channelName);
                actSignupFactModal.setSignupType("BM");
                actSignupFactModal.setSourceLastUpdatetime(modifiedTime);
                String currentDateTime = DateTimeUtils.getCurrentDateTime();
                actSignupFactModal.setSystemCreatetime(currentDateTime);
                actSignupFactModal.setSystemLastUpdatetime(currentDateTime);
                return actSignupFactModal;
            }
        });
        // 创建 Doris Sink
        DorisSink<ActSignupFactModal> actSignupFactModalDorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_ACT_SIGNUP_FACT");
        source.sinkTo(actSignupFactModalDorisSink);
        env.execute("TOdsHytdTManageProductNamelistToDwd");
    }
}
