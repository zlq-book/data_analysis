package com.travelsky.dataplatform.main.dwd.lygj;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.LygjCreateToSql;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.transform.lygj.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Timer;

import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.isKeyAccount;

public class ExtractLygjDpiToDwd {

    //private static final Logger logger = LoggerFactory.getLogger(ExtractLygjDpiToDwd.class);

    public static void main(String[] args) throws Exception {
        //IdMapping使用批量插入，开启定时器
        Timer timer = IdMapping.autoCommit();
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String startDateStr = etlDate;
        String endDateStr = etlDate;
        if (args.length > 2) {
            startDateStr = args[1];
            endDateStr = args[2];
        }
        // 定义日期格式
        //DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        // 解析日期
        //LocalDate startDate = LocalDate.parse(startDateStr, formatter);
        //LocalDate endDate = LocalDate.parse(endDateStr, formatter);
        //
        //etlDate = date.format(formatter);
        System.out.println("开始读取日期为：" + etlDate + "的DPI数据开始处理...");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointUtils.setCheckpoint(env, "ExtractLygjDpiToDwd");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);

        // 1. 创建Doris目标表
        tEnv.executeSql(LygjCreateToSql.DEPART_PASSENGER_INFO);
        // 2. 从Doris表中读取数据
        String query = "SELECT \n" +
                "ETL_DATE,\n" +
                "PSG_NAME_EN,\n" +
                "PSG_NAME_CN,\n" +
                "FLT_NUM,\n" +
                "FLT_DATE,\n" +
                "PNR_ICS,\n" +
                "SELL_CLASS,\n" +
                "FFP,\n" +
                "ET_NUM,\n" +
                "SPML_ITEM,\n" +
                "GENDER,\n" +
                "SEAT_NO,\n" +
                "STATUS,\n" +
                "BAGS,\n" +
                "BAGWHT,\n" +
                "CKI_PID,\n" +
                "ORIG,\n" +
                "DEST,\n" +
                "IS_FFP_JK,\n" +
                "IS_UM,\n" +
                "UPDATE_TIME,\n" +
                "CREATE_TIME,\n" +
                "BIRTHDAY,\n" +
                "RECORD_CREATE_TIME,\n" +
                "CERT_NO,\n" +
                "IS_VVIP,\n" +
                "CKI_TYPE,\n" +
                "ID_TYPE,\n" +
                "MC_FLT,\n" +
                "IO_FLAG,\n" +
                "EDI_FLAG,\n" +
                "PRINT_TICKET_TIME,\n" +
                "CHECK_IN_TIME,\n" +
                "BOOK_OFFI,\n" +
                "LKT_STATUS,\n" +
                "CKI_STATUS,\n" +
                "COUPON_STATUS,\n" +
                "ORIGINALTKNE,\n" +
                "MARKGP,\n" +
                "BIG_CUTNUM,\n" +
                "GDS_CODE,\n" +
                "TKNEGP,\n" +
                "DCS_DPTM,\n" +
                "DCS_ATTM,\n" +
                "PAX_FFP,\n" +
                "IS_GROUP,\n" +
                "PASSENGER_NUMBER,\n" +
                "DCS_SELL_CLASS,\n" +
                "ISSUE_OFFI,\n" +
                "DCS_ATDATE,\n" +
                "ORIG_SUB_CLASS,\n" +
                "TICKET_TYPE,\n" +
                "ISSUE_COUNTRY,\n" +
                "RESERVE_SEAT_NO,\n" +
                "FARE_BASIS_CODE,\n" +
                "IS_CHILD,\n" +
                "IS_INFANT,\n" +
                "MOBILE_PHONE,\n" +
                "CKI_AGENT,\n" +
                "EMAIL\n" +
                "FROM T_ODS_LYGJ_DEPART_PASSENGER_INFO " +
                "WHERE ETL_DATE >='" + startDateStr + "'"  + " AND ETL_DATE <='" + endDateStr + "'";
        System.out.println("日期为：" + etlDate + "的DPI数据开始查询...");
        Table table = tEnv.sqlQuery(query);
        DataStream<JSONObject> dpiSource = tEnv.toChangelogStream(table).map(new MapFunction<Row, JSONObject>() {
                    @Override
                    public JSONObject map(Row row) throws Exception {
                        // 证件号
                        String certNo = row.getFieldAs("CERT_NO");
                        if (StringUtils.isBlank(certNo)) {
                            return null;
                        }
                        // 证件号标准化
                        certNo = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, certNo, DataSource.LUYAN_STEWARD);

                        // 证件类型
                        String idType = row.getFieldAs("ID_TYPE");
                        // 证件类型标准化
                        idType = NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, idType, DataSource.LUYAN_STEWARD);

                        // 常客卡号
                        String ffp = row.getFieldAs("FFP");
                        // 常客卡号标准化
                        String ffpNo = NormalizationUtils.standardize(FieldType.FREQUENT_FLYER_NUM, ffp, DataSource.LUYAN_STEWARD);
                        // 生成TID
                        Tid tid = new Tid();
                        tid.setTid(certNo);
                        Map<String, String> certification = new HashMap<>();
                        certification.put(idType, certNo);
                        //tid.setFrequentTravelerCardno(ffpNo);
                        tid.setCertification(certification);
                        String tidStr = IdMapping.idMappingFunction(tid, "LYGJ", false,true,true);

                        // 性别
                        String gender = row.getFieldAs("GENDER");
                        // 性别标准化
                        gender = NormalizationUtils.standardize(FieldType.GENDER, gender, DataSource.LUYAN_STEWARD);

                        // 英文名
                        String psgNameEn = row.getFieldAs("PSG_NAME_EN");
                        psgNameEn = NormalizationUtils.standardize(FieldType.EN_NAME, psgNameEn, DataSource.LUYAN_STEWARD);
                        // 中文名
                        String psgNameCn = NormalizationUtils.standardize(FieldType.CN_NAME, row.getFieldAs("PSG_NAME_CN"), DataSource.LUYAN_STEWARD);

                        // 生日
                        String birthday = row.getFieldAs("BIRTHDAY");

                        // VVIP标识
                        String isVvip = row.getFieldAs("IS_VVIP");
                        // VVIP标识标准化
                        isVvip = NormalizationUtils.standardize(FieldType.VVIP_FLAG, isVvip, DataSource.LUYAN_STEWARD);

                        // 常客等级
                        String isFfpJk = row.getFieldAs("IS_FFP_JK");
                        // 常客等级标准化
                        isFfpJk = NormalizationUtils.standardize(FieldType.FREQUENT_FLYER_LEVEL, isFfpJk, DataSource.LUYAN_STEWARD);

                        // 大客户信息
                        String bigCutNum = row.getFieldAs("BIG_CUTNUM");
                        boolean isKeyAccount = isKeyAccount(bigCutNum);

                        // 乘机人年龄计算
                        //Long passengerAge = getAgeByIdCard(SM4Utils.decrypt(certNo, Constants.SM4_KEY), birthday) != null ?
                        //        getAgeByIdCard(certNo, birthday).longValue() : null;

                        // 国内国际标识
                        String ticketType = row.getFieldAs("TICKET_TYPE");
                        // 国内国际标识标准化
                        ticketType = NormalizationUtils.standardize(FieldType.INTERNATIONAL_DOMESTIC_FLAG, ticketType, DataSource.LUYAN_STEWARD);
                        // 儿童标识
                        String isChild = row.getFieldAs("IS_CHILD");
                        // 婴儿标识
                        String isInfant = row.getFieldAs("IS_INFANT");
                        // 值机Agent
                        String ckiAgent = row.getFieldAs("CKI_AGENT");
                        String mobilePhone = row.getFieldAs("MOBILE_PHONE");
                        mobilePhone = NormalizationUtils.standardize(FieldType.MOBILE_NO, mobilePhone, DataSource.LUYAN_STEWARD);
                        String etNum = NormalizationUtils.standardize(FieldType.TICKET_NO, row.getFieldAs("ET_NUM"), DataSource.LUYAN_STEWARD);


                        JSONObject map = new JSONObject();
                        // 添加所有字段到map
                        map.put("TID", tidStr);
                        map.put("ETL_DATE", row.getFieldAs("ETL_DATE"));
                        map.put("PSG_NAME_EN", psgNameEn);
                        map.put("PSG_NAME_CN", psgNameCn);
                        map.put("FLT_NUM", row.getFieldAs("FLT_NUM"));
                        map.put("FLT_DATE", row.getFieldAs("FLT_DATE"));
                        map.put("PNR_ICS", row.getFieldAs("PNR_ICS"));
                        map.put("SELL_CLASS", row.getFieldAs("SELL_CLASS"));
                        map.put("FFP", SM4Utils.decrypt(ffp, Constants.SM4_KEY));
                        map.put("ET_NUM", etNum);
                        map.put("SPML_ITEM", row.getFieldAs("SPML_ITEM"));
                        map.put("GENDER", gender);
                        map.put("SEAT_NO", row.getFieldAs("SEAT_NO"));
                        map.put("STATUS", row.getFieldAs("STATUS"));
                        map.put("BAGS", row.getFieldAs("BAGS"));
                        map.put("BAGWHT", row.getFieldAs("BAGWHT"));
                        map.put("CKI_PID", row.getFieldAs("CKI_PID"));
                        map.put("ORIG", row.getFieldAs("ORIG"));
                        map.put("DEST", row.getFieldAs("DEST"));
                        map.put("IS_FFP_JK", isFfpJk);
                        map.put("IS_UM", row.getFieldAs("IS_UM"));
                        map.put("UPDATE_TIME", row.getFieldAs("UPDATE_TIME"));
                        map.put("CREATE_TIME", row.getFieldAs("CREATE_TIME"));
                        map.put("BIRTHDAY", birthday);
                        map.put("RECORD_CREATE_TIME", row.getFieldAs("RECORD_CREATE_TIME"));
                        map.put("CERT_NO", certNo);
                        map.put("IS_VVIP", isVvip);
                        String ckiType = row.getFieldAs("CKI_TYPE");
                        ckiType = NormalizationUtils.standardize(FieldType.CHECK_IN_METHOD, ckiType, DataSource.LUYAN_STEWARD);
                        map.put("CKI_TYPE", ckiType);
                        map.put("ID_TYPE", idType);
                        map.put("MC_FLT", row.getFieldAs("MC_FLT"));
                        map.put("IO_FLAG", row.getFieldAs("IO_FLAG"));
                        map.put("EDI_FLAG", row.getFieldAs("EDI_FLAG"));
                        map.put("PRINT_TICKET_TIME", row.getFieldAs("PRINT_TICKET_TIME"));
                        map.put("CHECK_IN_TIME", row.getFieldAs("CHECK_IN_TIME"));
                        map.put("BOOK_OFFI", row.getFieldAs("BOOK_OFFI"));
                        map.put("LKT_STATUS", row.getFieldAs("LKT_STATUS"));
                        map.put("CKI_STATUS", row.getFieldAs("CKI_STATUS"));
                        map.put("COUPON_STATUS", row.getFieldAs("COUPON_STATUS"));
                        map.put("ORIGINALTKNE", row.getFieldAs("ORIGINALTKNE"));
                        map.put("MARKGP", row.getFieldAs("MARKGP"));
                        map.put("BIG_CUTNUM", bigCutNum);
                        map.put("GDS_CODE", row.getFieldAs("GDS_CODE"));
                        map.put("TKNEGP", row.getFieldAs("TKNEGP"));
                        map.put("DCS_DPTM", row.getFieldAs("DCS_DPTM"));
                        map.put("DCS_ATTM", row.getFieldAs("DCS_ATTM"));
                        map.put("PAX_FFP", SM4Utils.decrypt(row.getFieldAs("PAX_FFP"), Constants.SM4_KEY));
                        map.put("IS_GROUP", row.getFieldAs("IS_GROUP"));
                        map.put("PASSENGER_NUMBER", row.getFieldAs("PASSENGER_NUMBER"));
                        map.put("DCS_SELL_CLASS", row.getFieldAs("DCS_SELL_CLASS"));
                        map.put("ISSUE_OFFI", row.getFieldAs("ISSUE_OFFI"));
                        map.put("DCS_ATDATE", row.getFieldAs("DCS_ATDATE"));
                        map.put("ORIG_SUB_CLASS", row.getFieldAs("ORIG_SUB_CLASS"));
                        map.put("TICKET_TYPE", ticketType);
                        map.put("ISSUE_COUNTRY", row.getFieldAs("ISSUE_COUNTRY"));
                        map.put("RESERVE_SEAT_NO", row.getFieldAs("RESERVE_SEAT_NO"));
                        map.put("FARE_BASIS_CODE", row.getFieldAs("FARE_BASIS_CODE"));
                        map.put("EMAIL", row.getFieldAs("EMAIL"));
                        map.put("IS_KEY_ACCOUNT", isKeyAccount);
                        //map.put("PASSENGER_AGE", passengerAge != null ? passengerAge.toString() : null);
                        map.put("IS_CHILD", row.getFieldAs("IS_CHILD"));
                        map.put("IS_INFANT", row.getFieldAs("IS_INFANT"));
                        map.put("USER_TYPE", StringUtils.isNotBlank(isChild) && isChild.equals("Y") ? "CHD" : StringUtils.isNotBlank(isInfant) && isInfant.equals("1") ? "INF" : "ADT");
                        map.put("CKI_AGENT", ckiAgent);
                        map.put("MOBILE_PHONE", mobilePhone);

                        return map;
                    }
                }).setParallelism(1)
                .filter(Objects::nonNull);
        //.map(map -> {IdMapping.close(); return map;});
        LygjDpiToDwdBookingPnrFactTrans.result(dpiSource);
        LygjDpiToDwdBookingSegFactTrans.result(dpiSource);
        LygjDpiToDwdCheckinSegFactTrans.result(dpiSource);
        LygjDpiToDwdDatechangeSegFactTrans.result(dpiSource);
        LygjDpiToDwdDepartSegFactTrans.result(dpiSource);
        LygjDpiToDwdRefundSegFactTrans.result(dpiSource);
        LygjDpiToDwdTickingSegFactTrans.result(dpiSource);
        LygjDpiToDwdTickingTicFactTrans.result(dpiSource);
        LygjDpiToUserDimFrans.result(dpiSource);
        LygjDpiToCertDimTrans.result(dpiSource);
        LygjDpiToMobileDimTrans.result(dpiSource);
        LygjDpiToSegDimTrans.result(dpiSource);
        //启动任务
        env.execute("ExtractLygjDpiToDwd");
        //IdMapping使用批量插入，flink结束后停止计时器、提交事务和关闭连接
        timer.cancel();
        IdMapping.close();
    }
}
