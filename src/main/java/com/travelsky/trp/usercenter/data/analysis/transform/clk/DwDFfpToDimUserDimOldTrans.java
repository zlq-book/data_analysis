package com.travelsky.trp.usercenter.data.analysis.transform.clk;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimSummaryModel;
import com.travelsky.trp.usercenter.data.analysis.transform.hsd.CommonOutputTags;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;

public class DwDFfpToDimUserDimOldTrans {
    static final Logger logger = LoggerFactory.getLogger(DwDFfpToDimUserDimOldTrans.class);

    // 常客注册事实表 24年之前旧数据
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
                        + ",SEX_SOURCE  \n"
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
                try {
                    if (resultSet.next()) {
                        enNameSource = resultSet.getString("EN_NAME_SOURCE");
                        cnNameSource = resultSet.getString("CN_NAME_SOURCE");
                        nationalitySource = resultSet.getString("NATIONALITY_SOURCE");
                        mobilePhoneSource = resultSet.getString("MOBILE_PHONE_SOURCE");
                        birthdaySource = resultSet.getString("BIRTHDAY_SOURCE");
                        sexSource = resultSet.getString("SEX_SOURCE");
                    }
                } catch (Exception e) {
                    resultSet.close();
                    logger.error("常客数仓异步查询用户源表维表异常", e);
                }
                resultSet.close();

                String currentDateTime = DateTimeUtils.getCurrentDateTime();
                if (row.getField("CN_LAST_NAME") != null && row.getField("CN_FST_NAME") != null) {
                    boolean overWrite = StringUtils.isEmpty(cnNameSource)
                            || UserDimPriorityLevel.cnName(cnNameSource, "LSCK");
                    if (overWrite) {
                        //新数据优先级较高，使用新数据覆盖
                        model.setCnName(row.getField("CN_LAST_NAME").toString() + row.getField("CN_FST_NAME").toString());
                        summaryModel.setCnNameSource("LSCK");
                        summaryModel.setCnNameUpdatetime(currentDateTime);
                    }
                }
                if (row.getField("EN_LAST_NAME") != null && row.getField("EN_FST_NAME") != null) {
                    boolean overWrite = StringUtils.isEmpty(enNameSource)
                            || UserDimPriorityLevel.enName(enNameSource, "LSCK");
                    if (overWrite) {
                        //新数据优先级较高，使用新数据覆盖
                        model.setEnName(row.getField("EN_LAST_NAME").toString() + row.getField("EN_FST_NAME").toString());
                        summaryModel.setEnNameSource("LSCK");
                        summaryModel.setEnNameUpdatetime(currentDateTime);
                    }

                }
                if (row.getField("BIRTH_DT") != null) {
                    boolean overWrite = StringUtils.isEmpty(birthdaySource)
                            || UserDimPriorityLevel.handleBirthday(birthdaySource, "LSCK");
                    if (overWrite) {
                        //新数据优先级较高，使用新数据覆盖
                        model.setBirthday(row.getField("BIRTH_DT").toString());
                        summaryModel.setBirthdaySource("LSCK");
                        summaryModel.setBirthdayUpdatetime(currentDateTime);
                    }
                }

                if (row.getField("GENDER_ID") != null) {
                    boolean overWrite = StringUtils.isEmpty(sexSource)
                            || !"SFZ".equals(sexSource);
                    if (overWrite) {
                        //新数据优先级较高，使用新数据覆盖
                        model.setSex(row.getField("GENDER_ID").toString());
                        summaryModel.setSexSource("LSCK");
                        summaryModel.setSexUpdatetime(currentDateTime);
                    }
                }

//                if (row.getField("PR_MOBILE") != null) {
//                    boolean overWrite = StringUtils.isEmpty(mobilePhoneSource)
//                            || UserDimPriorityLevel.mobilePhone(mobilePhoneSource, "LSCK");
//                    if (overWrite) {
//                        //新数据优先级较高，使用新数据覆盖
//                        model.setMobilePhone(row.getField("PR_MOBILE").toString());
//                        summaryModel.setMobilePhoneSource("LSCK");
//                        summaryModel.setMobilePhoneUpdatetime(currentDateTime);
//                    }
//                }

                // ID
                model.setPkId(row.getField("TID").toString());
                summaryModel.setPkId(row.getField("TID").toString());

                // TID
                model.setHistoricalFfpId(row.getField("FFP_ID").toString());
                summaryModel.setHistoricalFfpIdSource("LSCK");
                summaryModel.setHistoricalFfpIdUpdatetime(currentDateTime);
                // 常旅客信息
//                model.setFrequentTraveler(true);
//                summaryModel.setIsFrequentTravelerSource("LSCK");
//                summaryModel.setIsFrequentTravelerUpdatetime(currentDateTime);
//                model.setFrequentTravelerCardno(row.getField("MEM_NUM").toString());
//                summaryModel.setFrequentTravelerCardnoSource("LSCK");
//                summaryModel.setFrequentTravelerCardnoUpdatetime(currentDateTime);
//                model.setFtRegisterTime(row.getField("FFP_CREATED").toString());
//                summaryModel.setFtRegisterTimeSource("LSCK");
//                summaryModel.setFtRegisterTimeUpdatetime(currentDateTime);

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
