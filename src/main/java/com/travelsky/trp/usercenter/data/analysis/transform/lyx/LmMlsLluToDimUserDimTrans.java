package com.travelsky.trp.usercenter.data.analysis.transform.lyx;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimSummaryModel;
import com.travelsky.trp.usercenter.data.analysis.transform.hsd.CommonOutputTags;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;


public class LmMlsLluToDimUserDimTrans {
    static final Logger logger = LoggerFactory.getLogger(LmMlsLluToDimUserDimTrans.class);

    public static void output(DataStream<Row> rowDataStream) {
        
        // 后续可以进行 map、filter、sink 操作 过滤证件号 是null
        SingleOutputStreamOperator<Void> processedStream = rowDataStream.filter(row -> {
            boolean flag = row.getField("MTM_CARD_NUM") == null;
            // 没有 ly card 无法获得主键
            if (flag) {
                logger.info("入库用户维表表异常，oriTable：LYX 三表联查，" + row.toString());
            }
            return !flag;
        }).process(new ProcessFunction<Row, Void>() {
            private transient Connection connDim;
            private transient Statement stmtDim;

            @Override
            public void open(Configuration parameters) throws Exception {
                connDim = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DIM_USER, Constants.DIM_PWD);
                stmtDim = DorisUtils.getStatement(connDim);
            }

            @Override
            public void close() throws Exception {
                DorisUtils.close(connDim, stmtDim, null);
            }

            @Override
            public void processElement(Row row, Context ctx, Collector<Void> out) throws Exception {
                UserDimModel model = new UserDimModel();
                // 用户信息概要维表(中间表)
                UserDimSummaryModel summaryModel = new UserDimSummaryModel();

                String firstSql = "SELECT  \n" +
                        "PK_ID , \n" +
                        "LY_USER_LEVEL  \n"
                        + " FROM "
                        + Constants.DIM_DB
                        + ".T_DIM_USER_DIM WHERE PK_ID='" + row.getField("TID").toString() + "'";
                ResultSet firstSet = DorisUtils.getDorisResult(stmtDim, firstSql);

                try {
                    if (null != firstSet && firstSet.next()) {
                        // 查询成功：补全维度信息
                        String level = firstSet.getString("LY_USER_LEVEL");
                        row.setField("OLD_LEVEL", level);

                    }
                } catch (SQLException e) {
                    firstSet.close();
                    logger.error("鲁雁行异步查询用户维表异常", e);
                }

                // 查询用户概要表
                String querySql = "SELECT \n"
                        + "CN_NAME_SOURCE\n"
                        + ",EN_NAME_SOURCE\n"
                        + ",NATIONALITY_SOURCE\n"
                        + ",MOBILE_PHONE_SOURCE\n"
                        + ",BIRTHDAY_SOURCE \n"
                        + " FROM "
                        + Constants.DIM_DB
                        + ".T_DIM_USER_DIM_SUMMARY WHERE PK_ID='" + row.getField("TID").toString() + "'";
                ResultSet resultSet = DorisUtils.getDorisResult(stmtDim, querySql);
                //中文名数据来源
                String cnNameSource = null;
                //英文名数据来源
                String enNameSource = null;
                //国籍数据来源
                String nationalitySource = null;
                //手机号数据来源
                String mobilePhoneSource = null;
                // 生日来源
                String birthdaySource = null;

                try {
                    if (resultSet.next()) {
                        cnNameSource = resultSet.getString("CN_NAME_SOURCE");
                        enNameSource = resultSet.getString("EN_NAME_SOURCE");
                        nationalitySource = resultSet.getString("NATIONALITY_SOURCE");
                        mobilePhoneSource = resultSet.getString("MOBILE_PHONE_SOURCE");
                        birthdaySource = resultSet.getString("BIRTHDAY_SOURCE");
                    }
                } catch (Exception e) {
                    if (null!=resultSet) {
                        resultSet.close();
                    }
                    logger.error("鲁雁行异步查询用户源表维表异常", e);
                }
                resultSet.close();

                String currentDateTime = DateTimeUtils.getCurrentDateTime();
                if (row.getField("MTM_SURNAME_CN") != null && row.getField("MTM_NAME_CN") != null) {
                    boolean overWrite = StringUtils.isEmpty(cnNameSource) || UserDimPriorityLevel.cnName(cnNameSource, "LYX");
                    if (overWrite) {
                        //新数据优先级较高，使用新数据覆盖
                        model.setCnName(row.getField("MTM_SURNAME_CN").toString() + row.getField("MTM_NAME_CN").toString());
                        summaryModel.setCnNameSource("LYX");
                        summaryModel.setCnNameUpdatetime(currentDateTime);
                    }

                }
                if (row.getField("MTM_SURNAME_EN") != null && row.getField("MTM_NAME_EN") != null) {
                    boolean overWrite = StringUtils.isEmpty(enNameSource) || UserDimPriorityLevel.enName(enNameSource, "LYX");
                    if (overWrite) {
                        //新数据优先级较高，使用新数据覆盖
                        model.setEnName(row.getField("MTM_SURNAME_EN").toString() + row.getField("MTM_NAME_EN").toString());
                        summaryModel.setEnNameSource("LYX");
                        summaryModel.setEnNameUpdatetime(currentDateTime);
                    }

                }
                if (row.getField("MTM_BIRTHDAY") != null) {
                    boolean overWrite = StringUtils.isEmpty(birthdaySource)
                            || UserDimPriorityLevel.handleBirthday(birthdaySource, "LYX");
                    if (overWrite) {
                        //新数据优先级较高，使用新数据覆盖
                        model.setBirthday(row.getField("MTM_BIRTHDAY").toString());
                        summaryModel.setBirthdaySource("LYX");
                        summaryModel.setBirthdayUpdatetime(currentDateTime);
                    }
                }

                if (row.getField("MTM_NATIONALITY") != null) {
                    boolean overWrite = StringUtils.isEmpty(nationalitySource)
                            || UserDimPriorityLevel.nationality(nationalitySource, "LYX");
                    if (overWrite) {
                        //新数据优先级较高，使用新数据覆盖
                        model.setNationality(row.getField("MTM_NATIONALITY").toString());
                        summaryModel.setNationalitySource("LYX");
                        summaryModel.setNationalityUpdatetime(currentDateTime);
                    }
                }

//                // 手机号 雁行实名认证标识是已认证的且认证方式不为LIP认证的手机号，或鲁雁行认证方式为LIP认证同时三要素认证结果是匹配的手机号。
//                if (null != row.getField("REALNAME_ATTESTATION") && null != row.getField("ATTESTATION_MODE")) {
//                    String newSource = "LYX";
//                    if ("1".equals(row.getField("REALNAME_ATTESTATION").toString())) {
//                        newSource = "LYXSM";
//                    }
//                    boolean firstFlag = "1".equals(row.getField("REALNAME_ATTESTATION").toString())
//                            // 雁行实名认证标识是已认证的且认证方式不为LIP认证的手机号
//                            && !"5".equals(row.getField("ATTESTATION_MODE").toString());
//                    boolean secondFlag = false;
//                    if (null != row.getField("LIP_ATTESTATION")) {
//                        // 鲁雁行认证方式为LIP认证同时三要素认证结果是匹配的手机号。
//                        secondFlag = "1".equals(row.getField("REALNAME_ATTESTATION").toString())
//                                && "5".equals(row.getField("ATTESTATION_MODE").toString())
//                                && "0".equals(row.getField("LIP_ATTESTATION").toString());
//                    }
//                    if (firstFlag || secondFlag) {
//                        newSource = "LYXSMS";
//                    }
//
//                    boolean overWrite = StringUtils.isEmpty(mobilePhoneSource)
//                            || UserDimPriorityLevel.mobilePhone(mobilePhoneSource, newSource);
//                    if (overWrite) {
//                        //新数据优先级较高，使用新数据覆盖
//                        model.setMobilePhone(row.getField("MTM_MOBILE").toString());
//                        summaryModel.setMobilePhoneSource(newSource);
//                        summaryModel.setMobilePhoneUpdatetime(currentDateTime);
//                    }
//
//                }

                // 证件写入 证件维表

                // TID
                model.setPkId(row.getField("TID").toString());
                summaryModel.setPkId(row.getField("TID").toString());

//                Integer nowLevel = row.getField("LEVEL_CODE") == null ? null : Integer.parseInt(NormalizationUtils.
//                        standardize(FieldType.LUNYANXING_USER_LEVEL, row.getField("LEVEL_CODE").toString()));
//                Integer oldLevel = row.getField("OLD_LEVEL") == null ? null : Integer.parseInt(NormalizationUtils.
//                        standardize(FieldType.LUNYANXING_USER_LEVEL, row.getField("OLD_LEVEL").toString()));
//                if (oldLevel == null || (nowLevel >= oldLevel)) {
//                    // 没有找到tid 找到tid 而且等级大于原来的 更新
//                    // 新旧 source都是 LYX
//                    model.setLyUser(row.getField("MTM_CARD_NUM") != null);
//                    summaryModel.setIsLyUserSource("LYX");
//                    summaryModel.setIsLyUserUpdatetime(currentDateTime);
//
//                    model.setLyRegisterTime(row.getField("MTM_REGISTER_TIME").toString());
//                    summaryModel.setIsLyUserSource("LYX");
//                    summaryModel.setIsLyUserUpdatetime(currentDateTime);
//
//                    model.setLyCardNumber(row.getField("MTM_CARD_NUM").toString());
//                    summaryModel.setLyCardNumberSource("LYX");
//                    summaryModel.setLyCardNumberUpdatetime(currentDateTime);
//
//                    model.setLyUserStatus(row.getField("MTM_STATUS").toString());
//                    summaryModel.setLyUserStatusSource("LYX");
//                    summaryModel.setLyUserStatusUpdatetime(currentDateTime);
//
//                    model.setLyRegisterStatus(row.getField("REGISTER_STATUS").toString());
//                    summaryModel.setLyRegisterStatusSource("LYX");
//                    summaryModel.setLyRegisterStatusUpdatetime(currentDateTime);
//
//                    model.setLyVerifyStatus(row.getField("REALNAME_ATTESTATION").toString());
//                    summaryModel.setLyVerifyStatusSource("LYX");
//                    summaryModel.setLyVerifyStatusUpdatetime(currentDateTime);
//
//                    model.setLyUserLevel(row.getField("LEVEL_CODE").toString());
//                    summaryModel.setLyUserLevelSource("LYX");
//                    summaryModel.setLyUserLevelUpdatetime(currentDateTime);
//
//                }
//                if (null != row.getField("CYCLE_LYVAL")) {
//                    model.setLyLifetimePoints((int) Double.parseDouble(row.getField("CYCLE_LYVAL").toString()));
//                    summaryModel.setLyLifetimePointsSource("LYX");
//                    summaryModel.setLyLifetimePointsUpdatetime(currentDateTime);
//                }
//
//                if (null != row.getField("USABLE_LYVAL")) {
//                    model.setLyAvailablePoints((int) Double.parseDouble(row.getField("USABLE_LYVAL").toString()));
//                    summaryModel.setLyAvailablePointsSource("LYX");
//                    summaryModel.setLyAvailablePointsUpdatetime(currentDateTime);
//                }
                model.setCreateTime(LocalDateTime.now().toString());
                model.setUpdateTime(LocalDateTime.now().toString());
                ctx.output(CommonOutputTags.USER_DIM_TAG, model);
                ctx.output(CommonOutputTags.USER_DIM_SUMMARY_TAG, summaryModel);

            }

        });


        // 输出结果查看
        DataStream<UserDimModel> dimStream = processedStream.getSideOutput(CommonOutputTags.USER_DIM_TAG);
        DataStream<UserDimSummaryModel> dimSummaryStream = processedStream.getSideOutput(CommonOutputTags.USER_DIM_SUMMARY_TAG);
        dimStream.print("dimStream");
        dimSummaryStream.print("dimSummaryStream");
        // 创建 Doris Sink 并写入
        dimStream.sinkTo(FlinkDorisUtils.creatDorisDIMSink("T_DIM_USER_DIM"));
        dimSummaryStream.sinkTo(FlinkDorisUtils.creatDorisDIMSink("T_DIM_USER_DIM_SUMMARY"));

    }
}
