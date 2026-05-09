package com.travelsky.dataplatform.main.dwd;

import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.AuisSegFactModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.CheckinSegFactModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * @author kuangaihua
 * @date 2025/8/4 15:10
 */
public class TOdsWxapWxCheckinInsureToDwd {
    static final Logger logger = LoggerFactory.getLogger(TOdsWxapWxCheckinInsureToDwd.class);

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
        tEnv.executeSql(GetTableSql.getQueryTOdsWxapWxCheckinInsure());
        //查询sql
        String query = "SELECT DEPDATE,\n" +
                "ETN,\n" +
                "DEP,\n" +
                "ARR,\n" +
                "ID,\n" +
                "CREATE_TIME,\n" +
                "CERTNAME,\n" +
                "CERTTYPE,\n" +
                "CERTNO,\n" +
                "UNIONID,\n" +
                "POLICYID,\n" +
                "CAST(POLICY_TYPE AS STRING) POLICY_TYPE,\n" +
                "CAST(CHANNEL_TYPE AS STRING) CHANNEL_TYPE,\n" +
                "CAST(OSTATUS AS STRING) OSTATUS,\n" +
                "CAST(PRICE AS DOUBLE) PRICE,\n" +
                "UPDATE_TIME\n" +
                "FROM T_ODS_WXAP_WX_CHECKIN_INSURE\n" +
                "WHERE ETL_DATE='" + etlDate + "'" +
                "AND ETN IS NOT NULL";
        logger.info("查询sql：" + query);
        Table table = tEnv.sqlQuery(query);
        // 转换为 DataStream<Row>
        DataStream<AuisSegFactModel> source = tEnv.toChangelogStream(table).map(
                row -> {
                    //出发时间
                    LocalDateTime depdate = row.getFieldAs("DEPDATE");
                    //客票号
                    Object etn = row.getField("ETN");
                    // 保单号
                    Object policyId = row.getField("POLICYID");
                    //起飞机场
                    String dep = row.getFieldAs("DEP");
                    //到达机场
                    String arr = row.getFieldAs("ARR");
                    //关联订单号
                    String id = row.getFieldAs("ID");
                    //创建时间
                    LocalDateTime createTime = row.getFieldAs("CREATE_TIME");
                    //姓名
                    String certname = row.getFieldAs("CERTNAME");
                    certname = NormalizationUtils.standardize(FieldType.CN_NAME, certname, DataSource.WECHAT_MINI_PROGRAM);
                    //证件类型
                    String certtype = row.getFieldAs("CERTTYPE");
                    logger.info("标准化前证件类型:certtype" + certtype);
                    certtype = NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, certtype, DataSource.WECHAT_MINI_PROGRAM);
                    logger.info("标准化后证件类型:certtype" + certtype);
                    //证件号码
                    String certno = row.getFieldAs("CERTNO");
                    certno = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, certno, DataSource.WECHAT_MINI_PROGRAM);
                    //customerId
                    String unionid = row.getFieldAs("UNIONID");
                    //保险类型
                    String policyType = row.getFieldAs("POLICY_TYPE");
                    //渠道类型
                    String channelType = row.getFieldAs("CHANNEL_TYPE");
                    //订单状态
                    String ostatus = row.getFieldAs("OSTATUS");
                    //保险价格
                    Double price = row.getFieldAs("PRICE");
                    //更新时间
                    LocalDateTime updateTime = row.getFieldAs("UPDATE_TIME");
                    //航班起飞日期
                    String flightDate = DateTimeUtils.localDateTimeToDateString(depdate);
                    //航班起飞时间
                    String flightTime = DateTimeUtils.localDateTimeToTimeString(depdate);
                    //保险预定日期
                    String fkBkauisDate = DateTimeUtils.localDateTimeToDateString(createTime);
                    //保险预定时间
                    String fkBkauisTime = DateTimeUtils.localDateTimeToTimeString(createTime);
                    // 取ETN+POLICYID。如果ETN或者POLICYID有一个为空，就以表的ID为主键。
                    String pkId = etn == null || policyId == null ? id : etn.toString() + policyId.toString();

                    String currentDateTime = DateTimeUtils.getCurrentDateTime();
                    AuisSegFactModel auisSegFactModel = new AuisSegFactModel();
                    auisSegFactModel.setPkId(pkId);
                    auisSegFactModel.setAkOrdernum(id);
                    auisSegFactModel.setAkTiknum(etn == null ? null : etn.toString());
                    auisSegFactModel.setFkBkauisDate(fkBkauisDate);
                    auisSegFactModel.setFkBkauisTime(fkBkauisTime);
                    auisSegFactModel.setCnName(certname);
                    auisSegFactModel.setAkCertType(certtype);
                    auisSegFactModel.setCertNumber(certno);
                    Tid tidPassenger = new Tid();
                    tidPassenger.setTid(certno);
                    Map<String, String> certification = new HashMap<>();
                    certification.put(certtype, certno);
                    tidPassenger.setCertification(certification);
                    String tidPassengerStr = IdMapping.idMappingFunction(tidPassenger, "WXAP");
                    Tid tidBook = new Tid();
                    tidBook.setTid(unionid);
                    tidBook.setCrmCustomerId(unionid);
                    String tidBookStr = IdMapping.idMappingFunction(tidPassenger, "WXAP");
                    auisSegFactModel.setFkPassengerUserTid(tidPassengerStr);
                    auisSegFactModel.setFkBookingUserTid(tidBookStr);
                    auisSegFactModel.setFkBookingUserOriginId(unionid);
                    auisSegFactModel.setFkSegDate(flightDate);
                    auisSegFactModel.setFkSegTime(flightTime);
                    auisSegFactModel.setFkDepairport(dep);
                    auisSegFactModel.setFkArriairport(arr);
                    // V2 保单号
                    auisSegFactModel.setInsurancePolicyNo(policyId == null ? null : policyId.toString());
                    if (policyType != null && (policyType.equals("0") || policyType.equals("1") || policyType.equals("3"))) {
                        auisSegFactModel.setAkInsuranceType("HYX");
                    } else {
                        auisSegFactModel.setAkInsuranceType(policyType);
                    }
                    auisSegFactModel.setAkChannel(NormalizationUtils.standardize(FieldType.INSURANCE_ORDER_CHANNEL,
                            channelType, DataSource.WECHAT_MINI_PROGRAM));
                    auisSegFactModel.setAkInsurstatus(NormalizationUtils.standardize(FieldType.INSURANCE_STATUS,
                            ostatus, DataSource.WECHAT_MINI_PROGRAM));
                    auisSegFactModel.setInsuranceAmt(price);
                    auisSegFactModel.setInsuranceCount(1L);
                    auisSegFactModel.setUpdateTime(DateTimeUtils.localDateTimeToString(updateTime));
                    auisSegFactModel.setSystemCreatetime(currentDateTime);
                    auisSegFactModel.setSystemLastUpdatetime(currentDateTime);
                    return auisSegFactModel;
                }
        );
        source.print();
        //  创建 Doris Sink 并写入
        DorisSink<AuisSegFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_AUIS_SEG_FACT");
        //数据写入doris
        source.sinkTo(dorisSink);
        env.execute("TOdsWxapWxCheckinInsureToDwd");
    }
}
