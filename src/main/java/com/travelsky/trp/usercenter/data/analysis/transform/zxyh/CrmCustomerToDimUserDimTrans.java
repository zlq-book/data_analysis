package com.travelsky.trp.usercenter.data.analysis.transform.zxyh;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.DateTimeUtils;
import com.travelsky.dataplatform.utils.DorisUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.UserDimPriorityLevel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.CustomDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimSummaryModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.RegisterUdoFactModel;
import com.travelsky.trp.usercenter.data.analysis.transform.hsd.CommonOutputTags;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;

public class CrmCustomerToDimUserDimTrans {

    static final Logger logger = LoggerFactory.getLogger(CrmCustomerToDimUserDimTrans.class);

    public static void output(DataStream<Row> rowDataStream) {

        // 后续可以进行 map、filter、sink 操作 过滤证件号 是null
        SingleOutputStreamOperator<Void> processedStream = rowDataStream.filter(row -> {
            boolean flag = row.getField("TID") == null;
            // 没有 ly card 无法获得主键
            if (flag) {
                logger.info("入库用户维表表异常，oriTable：常客数仓，" + row.toString());
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

                // 查询用户概要表
                String querySql = "SELECT \n"
                        + "CN_NAME_SOURCE\n"
                        + ",EN_NAME_SOURCE\n"
                        + ",NATIONALITY_SOURCE\n"
                        + ",MOBILE_PHONE_SOURCE\n"
                        + ",BIRTHDAY_SOURCE \n"
                        + ",SEX_SOURCE \n"
                        + ",PROVINCE_SOURCE \n"
                        + ",ETHNICITY_SOURCE \n"
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
                // 性别来源
                String sexSource = null;
                String provinceSource = null;
                String ethnicitySource = null;
                try {
                    if (resultSet.next()) {
                        enNameSource = resultSet.getString("EN_NAME_SOURCE");
                        cnNameSource = resultSet.getString("CN_NAME_SOURCE");
                        nationalitySource = resultSet.getString("NATIONALITY_SOURCE");
                        mobilePhoneSource = resultSet.getString("MOBILE_PHONE_SOURCE");
                        birthdaySource = resultSet.getString("BIRTHDAY_SOURCE");
                        sexSource = resultSet.getString("SEX_SOURCE");
                        provinceSource = resultSet.getString("PROVINCE_SOURCE");
                        ethnicitySource = resultSet.getString("ETHNICITY_SOURCE");
                    }
                } catch (Exception e) {
                    if (null != resultSet) {
                        resultSet.close();
                    }
                    logger.error("常客数仓异步查询用户源表维表异常", e);
                }
                if (null != resultSet) {
                    resultSet.close();
                }

                String currentDateTime = DateTimeUtils.getCurrentDateTime();

                if (null != row.getField("CN_NAME")) {
                    boolean overWrite = StringUtils.isEmpty(cnNameSource)
                            || UserDimPriorityLevel.cnName(cnNameSource, "ZXYH");
                    if (overWrite) {
                        //新数据优先级较高，使用新数据覆盖
                        model.setCnName(row.getField("CN_NAME").toString());                        
                        summaryModel.setCnNameSource("ZXYH");
                        summaryModel.setCnNameUpdatetime(currentDateTime);
                    }
                } else if (null != row.getField("LAST_CN_NAME") && null != row.getField("FIRST_CN_NAME")){
                    boolean overWrite = StringUtils.isEmpty(cnNameSource)
                            || UserDimPriorityLevel.cnName(cnNameSource, "ZXYH");
                    if (overWrite) {
                        //新数据优先级较高，使用新数据覆盖
                        model.setCnName(row.getField("LAST_CN_NAME").toString() + row.getField("FIRST_CN_NAME").toString());
                        summaryModel.setCnNameSource("ZXYH");
                        summaryModel.setCnNameUpdatetime(currentDateTime);
                    }
                }
                // 英文名
                if (null != row.getField("ENG_NAME")) {
                    boolean overWrite = StringUtils.isEmpty(enNameSource)
                            || UserDimPriorityLevel.enName(enNameSource, "ZXYH");
                    if (overWrite) {
                        //新数据优先级较高，使用新数据覆盖
                        model.setEnName(row.getField("ENG_NAME").toString());
                        summaryModel.setEnNameSource("ZXYH");
                        summaryModel.setEnNameUpdatetime(currentDateTime);
                    }
                } else if (null != row.getField("LAST_ENG_NAME") && null != row.getField("FIRST_ENG_NAME")){
                    boolean overWrite = StringUtils.isEmpty(enNameSource)
                            || UserDimPriorityLevel.enName(enNameSource, "ZXYH");
                    if (overWrite) {
                        //新数据优先级较高，使用新数据覆盖
                        model.setEnName(row.getField("LAST_ENG_NAME").toString() + row.getField("FIRST_ENG_NAME").toString());
                        summaryModel.setEnNameSource("ZXYH");
                        summaryModel.setEnNameUpdatetime(currentDateTime);
                    }
                }
                // 生日
                if (row.getField("BIRTHDAY") != null) {
                    boolean overWrite = StringUtils.isEmpty(birthdaySource)
                            || UserDimPriorityLevel.handleBirthday(birthdaySource, "ZXYH");
                    if (overWrite) {
                        //新数据优先级较高，使用新数据覆盖
                        model.setBirthday(row.getField("BIRTHDAY").toString());
                        summaryModel.setBirthdaySource("ZXYH");
                        summaryModel.setBirthdayUpdatetime(currentDateTime);
                    }
                }
                // 性别
                if (row.getField("SEX") != null) {
                    boolean overWrite = StringUtils.isEmpty(sexSource)
                            || !"SFZ".equals(sexSource);
                    if (overWrite) {
                        //新数据优先级较高，使用新数据覆盖
                        model.setSex(row.getField("SEX").toString());
                        summaryModel.setSexSource("ZXYH");
                        summaryModel.setSexUpdatetime(currentDateTime);
                    }
                }

//                if (row.getField("PROVINCE") != null) {
//                    boolean overWrite = StringUtils.isEmpty(provinceSource)
//                            || UserDimPriorityLevel.province(provinceSource, "ZXYH");
//                    if (overWrite) {
//                        //新数据优先级较高，使用新数据覆盖
//                        model.setProvince(row.getField("PROVINCE").toString());
//                        summaryModel.setProvinceSource("ZXYH");
//                        summaryModel.setProvinceUpdatetime(currentDateTime);
//                    }
//                }
                // 国籍
                if (row.getField("NATIONAL") != null) {
                    boolean overWrite = StringUtils.isEmpty(nationalitySource)
                            || UserDimPriorityLevel.nationality(nationalitySource, "ZXYH");
                    if (overWrite) {
                        //新数据优先级较高，使用新数据覆盖
                        model.setNationality(row.getField("NATIONAL").toString());
                        summaryModel.setNationalitySource("ZXYH");
                        summaryModel.setNationalityUpdatetime(currentDateTime);
                    }
                }
                // 民族
                if (row.getField("ETHNIC_GROUP") != null) {
                    boolean overWrite = StringUtils.isEmpty(ethnicitySource)
                            || UserDimPriorityLevel.ethnicity(ethnicitySource, "ZXYH");
                    if (overWrite) {
                        //新数据优先级较高，使用新数据覆盖
                        model.setEthnicity(row.getField("ETHNIC_GROUP").toString());
                        summaryModel.setEthnicitySource("ZXYH");
                        summaryModel.setEthnicityUpdatetime(currentDateTime);
                    }
                }
                // 手机
//                String tag = "ZXYH";
//                if (row.getField("REAL_NAME_FLAG") != null
//                        && "1".equals(row.getField("REAL_NAME_FLAG").toString())) {
//                    // 当前手机号最高优先级
//                    if (null != row.getField("REAL_NAME_TYPE") &&
//                            ("1".equals(row.getField("REAL_NAME_TYPE").toString()) ||
//                                    "0".equals(row.getField("REAL_NAME_TYPE").toString()))) {
//                        tag = "ZXYHSMS";
//                    } else {
//                        tag = "ZXYHSM";
//                    }
//                }
//                if (row.getField("CONTACT_NUMBER") != null) {
//                    boolean overWrite = StringUtils.isEmpty(mobilePhoneSource)
//                            || UserDimPriorityLevel.mobilePhone(mobilePhoneSource, tag);
//                    if (overWrite) {
//                        //新数据优先级较高，使用新数据覆盖
//                        //  手机号 关联联系表
//                        model.setMobilePhone(row.getField("CONTACT_NUMBER").toString());
//                        summaryModel.setMobilePhoneSource(tag);
//                        summaryModel.setMobilePhoneUpdatetime(currentDateTime);
//                    }
//                }

                // ID
                model.setPkId(row.getField("TID").toString());
                summaryModel.setPkId(row.getField("TID").toString());

                // TID
                model.setCrmCustomerId(row.getField("CUSTOMER_ID").toString());
                summaryModel.setHistoricalFfpIdSource("ZXYH");
                summaryModel.setHistoricalFfpIdUpdatetime(currentDateTime);
                // 直销用户信息
                if ("1".equals(row.getField("CUSTOMER_STATUS").toString())) {
                    model.setDirectUser(true);
                    summaryModel.setIsDirectUserSource("ZXYH");
                    summaryModel.setIsDirectUserUpdatetime(currentDateTime);
                }
//                model.setDirectUserStatus(row.getField("CUSTOMER_STATUS").toString());
//                summaryModel.setDirectUserStatusSource("ZXYH");
//                summaryModel.setDirectUserStatusUpdatetime(currentDateTime);
//                if (null != row.getField("LAST_LOGIN_TIME")) {
//                    model.setLastLoginTime(row.getField("LAST_LOGIN_TIME").toString());
//                }


//                if (null != row.getField("REAL_NAME_FLAG")
//                        && "1".equals(row.getField("REAL_NAME_FLAG").toString())) {
//                    model.setDirectVerifiedFlag(true);
//                    summaryModel.setDirectVerifiedFlagSource("ZXYH");
//                    summaryModel.setDirectVerifiedFlagUpdatetime(currentDateTime);
//                }
//                if (null != row.getField("REAL_NAME_TIME")) {
//                    model.setDirectVerifyDate(row.getField("REAL_NAME_TIME").toString());
//                    summaryModel.setDirectVerifyDateSource("ZXYH");
//                    summaryModel.setDirectVerifyDateUpdatetime(currentDateTime);
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
