package com.travelsky.dataplatform.main.dwd;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.transform.hytd.*;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;

/**
 * @author kuangaihua
 * @date 2025/8/4 15:10
 */
public class TOdsHytdSysRegisterToDwd {
    static final Logger logger = LoggerFactory.getLogger(TOdsHytdSysRegisterToDwd.class);

    public static void main(String[] args) throws Exception {
        //IdMapping使用批量插入，开启定时器
//        Timer timer = IdMapping.autoCommit();
        if (args.length < 1) {
            logger.error("etl_date参数为空");
            System.exit(0);
        }

        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String startDateStr = etlDate;
        String endDateStr = etlDate;
        if (args.length > 1) {
            startDateStr = args[0];
            endDateStr = args[1];
        }
        logger.info("etl_date:" + etlDate);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointUtils.setCheckpoint(env, "TOdsHytdSysRegisterToDwd");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //注册数据源表
        tEnv.executeSql(GetTableSql.getQueryTOdsHytdSysRegister());
        //查询sql
        String query = "SELECT \n" +
                "CREATE_DATE,\n" +
                "MEMBER_NUMBER,\n" +
                "GENDER,\n" +
                "CREDENTIAL_NUM,\n" +
                "CREDENTIAL_TYPE,\n" +
                "COALESCE(FIRST_NAME, '') FIRST_NAME,\n" +
                "COALESCE(LAST_NAME, '') LAST_NAME,\n" +
                "COALESCE(CN_FIRST_NAME, '') CN_FIRST_NAME,\n" +
                "COALESCE(CN_LAST_NAME, '') CN_LAST_NAME,\n" +
                "BIRTHDAY,\n" +
                "NATIONALITY,\n" +
                "`LANGUAGE`,\n" +
                "PARENT_MEMBER_NUM,\n" +
                "SUBMIT_PERSON,\n" +
                "CHANNEL_ID,\n" +
                "FAST_CREATE_FLAG,\n" +
                "CRM_MEMBER_ID,\n" +
                "`STATE`,\n" +
                "CITY,\n" +
                "NATIONALITY,\n" +
                "COMPANY,\n" +
                "PHONE_NUM,\n" +
                "EMAIL_ADDR\n" +
                " FROM T_ODS_HYTD_SYS_REGISTER " +
                "WHERE ETL_DATE >='" + startDateStr + "'"  + " AND ETL_DATE <='" + endDateStr + "' "+
                " AND REGISTER_STATUS = '1' "
//                " ORDER BY CREATE_DATE"
                ;
        logger.info("查询sql：" + query);
        Table table = tEnv.sqlQuery(query);
        // 转换为 DataStream<Map<String, String>>，生成tid并做标准化
        DataStream<JSONObject> source = tEnv.toChangelogStream(table).map(new RichMapFunction<Row, JSONObject>() {
            Timer timer;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                timer=IdMapping.autoCommit();
            }

            @Override
            public void close() throws Exception {
                super.close();
                timer.cancel();
                IdMapping.close();
            }

            @Override
            public JSONObject map(Row row) throws Exception {
                //常客注册时间
                String createDate=DateTimeUtils.localDateTimeToString(row.getFieldAs("CREATE_DATE"));
                String credentialType=row.getFieldAs("CREDENTIAL_TYPE");
                logger.info("标准化前证件类型:credentialType"+credentialType);
                //证件类型标准化
                credentialType=NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, credentialType,DataSource.MEMBER_WORLD);
                logger.info("标准化后证件类型:credentialType"+credentialType);
                //证件号
                String credentialNum=row.getFieldAs("CREDENTIAL_NUM");
                logger.info("标准化前证件号:credentialNum"+credentialNum);
                //证件号标准化
                credentialNum=NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, credentialNum,DataSource.MEMBER_WORLD);
                logger.info("标准化后证件号:credentialNum"+credentialNum);
                //常客卡号
                String memberNumber=row.getFieldAs("MEMBER_NUMBER");
                //常客卡号标准化
                memberNumber=NormalizationUtils.standardize(FieldType.FREQUENT_FLYER_NUM, memberNumber,DataSource.MEMBER_WORLD);
                //性别
                String gender=row.getFieldAs("GENDER");
                //性别标准化
                gender=NormalizationUtils.standardize(FieldType.GENDER, gender,DataSource.MEMBER_WORLD);
                //英文名
                String firstName=row.getFieldAs("FIRST_NAME");
                //英文姓
                String lastName=row.getFieldAs("LAST_NAME");
                //中文名
                String cnFirstName = row.getFieldAs("CN_FIRST_NAME");
                //中文姓
                String cnLastName = row.getFieldAs("CN_LAST_NAME");
                //生日
                String birthday = row.getFieldAs("BIRTHDAY");
                //国籍
                String nationality = row.getFieldAs("NATIONALITY");
                //国籍标准化
                nationality = NormalizationUtils.standardize(FieldType.NATIONALITY, nationality,DataSource.MEMBER_WORLD);
                //联系语言
                String language = row.getFieldAs("LANGUAGE");
                //父母常客卡号
                String parentMemberNum = row.getFieldAs("PARENT_MEMBER_NUM");
                //父母常客卡号标准化
                parentMemberNum = NormalizationUtils.standardize(FieldType.FREQUENT_FLYER_NUM, parentMemberNum,DataSource.MEMBER_WORLD);
                //发展人
                String submitPerson = row.getFieldAs("SUBMIT_PERSON");
                //渠道号，会员天地分配
                String channelId = row.getFieldAs("CHANNEL_ID");
                //快速创建会员标识
                String fastCreateFlag = row.getFieldAs("FAST_CREATE_FLAG");
                //会员天地用户ID
                String crmMemberId = row.getFieldAs("CRM_MEMBER_ID");
                //省份
                String state = row.getFieldAs("STATE");
                //省份标准化
                state = NormalizationUtils.standardize(FieldType.PROVINCE, state,DataSource.MEMBER_WORLD);
                //城市
                String city = row.getFieldAs("CITY");
                //城市标准化
                city = NormalizationUtils.standardize(FieldType.CITY, city,DataSource.MEMBER_WORLD);
                //公司
                String company = row.getFieldAs("COMPANY");
                //电话号码
                String phoneNum = row.getFieldAs("PHONE_NUM");
                //电话号码标准化
                phoneNum = NormalizationUtils.standardize(FieldType.MOBILE_NO, phoneNum,DataSource.MEMBER_WORLD);
                //邮箱
                String emailAddr = row.getFieldAs("EMAIL_ADDR");
                //中文名
                String cnName=cnLastName+cnFirstName;
                //中文姓名标准化
                cnName=NormalizationUtils.standardize(FieldType.CN_NAME, cnName,DataSource.MEMBER_WORLD);
                //英文名
                String enName=lastName+"/"+firstName;
                //英文名标准化
                enName=NormalizationUtils.standardize(FieldType.EN_NAME, enName,DataSource.MEMBER_WORLD);
                JSONObject map = new JSONObject();
                map.put("CREATE_DATE", createDate);
                map.put("MEMBER_NUMBER", memberNumber);
                map.put("GENDER", gender);
                map.put("CREDENTIAL_NUM", credentialNum);
                map.put("CREDENTIAL_TYPE", credentialType);
                map.put("FIRST_NAME", firstName);
                map.put("LAST_NAME", lastName);
                map.put("CN_FIRST_NAME", cnFirstName);
                map.put("CN_LAST_NAME", cnLastName);
                map.put("BIRTHDAY", birthday);
                map.put("NATIONALITY", nationality);
                map.put("LANGUAGE", language);
                map.put("PARENT_MEMBER_NUM", parentMemberNum);
                map.put("SUBMIT_PERSON", submitPerson);
                map.put("CHANNEL_ID", channelId);
                map.put("FAST_CREATE_FLAG", fastCreateFlag);
                map.put("CRM_MEMBER_ID", crmMemberId);
                map.put("STATE", state);
                map.put("CITY", city);
                map.put("COMPANY", company);
                map.put("PHONE_NUM", phoneNum);
                map.put("EMAIL_ADDR", emailAddr);
                map.put("CN_NAME", cnName);
                map.put("EN_NAME", enName);
                Tid tid = new Tid();
                tid.setTid(credentialNum);
                Map<String, String> certification = new HashMap<>();
                certification.put(credentialType, credentialNum);
                tid.setFrequentTravelerCardno(memberNumber);
                tid.setCertification(certification);
                String tidStr = IdMapping.idMappingFunction(tid, "HYTD",false,true,true);
                map.put("TID", tidStr);
                return map;
            }
        });
//        source.print("数据标准化完成");
        //写用户维表
        SysRegisterToUserDimTrans.result(source);
        //写手机号维表
        SysRegisterToMobileDimTrans.result(source);
        //写证件信息维表
        SysRegisterToCertDimTrans.result(source);
        //写常旅客信息维表
        SysRegisterToFfpDimTrans.result(source);
        //写常旅客注册事实表
        SysRegisterToMemberRegisterFactTrans.result(source);
        env.execute("TOdsHytdSysRegisterToDwd");
        //IdMapping使用批量插入，flink结束后停止计时器、提交事务和关闭连接
//        timer.cancel();
        IdMapping.close();
    }
}
