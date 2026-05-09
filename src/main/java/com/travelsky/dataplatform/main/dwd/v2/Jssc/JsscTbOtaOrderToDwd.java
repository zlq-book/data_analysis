package com.travelsky.dataplatform.main.dwd.v2.Jssc;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.UpgradeCouponSalesFactModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * 基于ODS的JSSC升舱券销售订单表(OTA销售)抽取至 升舱-券销售-业务事实表 DWD层
 */
public class JsscTbOtaOrderToDwd { // 定义JsscTbOtaOrderToDwd类

    public static void main(String[] args) throws Exception { // 主函数入口，抛出异常
        if (args.length < 1) { // 判断入参长度是否小于1
            System.exit(0); // 没有传入etl_date参数则退出程序
        } // if结束
        String etlDate = args[0]; // 获取etl_date参数

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); // 创建Flink流执行环境
        CheckpointUtils.setCheckpoint(env, "JsscTbOtaOrderToDwd"); // 设置Checkpoint，便于任务容错与重启
        env.setParallelism(1); // 设置并行度为1，便于调试与串行输出
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env); // 创建Table环境

        // 1. 创建Doris目标表 - TB_OTA_ORDER升舱券销售订单表(OTA销售)
        String TB_OTA_ORDER = "CREATE TABLE T_ODS_JSSC_TB_OTA_ORDER (\n" + // 构建T_ODS_JSSC_TB_OTA_ORDER建表语句开头
                "    ID               BIGINT,\n" + // ID
                "    ORDER_NO         VARCHAR(30),\n" + // 升舱券销售订单号
                "    SELL_DATE        TIMESTAMP(3),\n" + // 销售日期时间
                "    CHANNEL          VARCHAR(30),\n" + // 销售渠道
                "    MOBILE           VARCHAR(30),\n" + // 购买人手机号
                "    STATUS           BIGINT\n" + // 券状态
                ") WITH (\n" + // 结束列定义并开始WITH子句
                GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_JSSC_TB_OTA_ORDER", Constants.ODS_USER, Constants.ODS_PWD) + // 使用工具方法拼接Doris连接器参数
                ")"; // 结束建表语句
        tEnv.executeSql(TB_OTA_ORDER); // 执行建表SQL

        //创建Doris目标表 - TB_UPGRADE_TICKET升舱券表
        String TB_UPGRADE_TICKET = "CREATE TABLE T_ODS_JSSC_TB_UPGRADE_TICKET (\n" + // 构建T_ODS_JSSC_TB_UPGRADE_TICKET建表语句开头
                "    ORDER_ID       BIGINT,\n" + // 升舱券销售订单号
                "    NUMBERS        VARCHAR(90),\n" + // 券号
                "    AMOUNT         BIGINT,\n" + // 金额
                "    STATUS         BIGINT,\n" + // 券状态
                "    USETIME        TIMESTAMP(3)\n" + // 使用时间
                ") WITH (\n" + // 结束列定义并开始WITH子句
                GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_JSSC_TB_UPGRADE_TICKET", Constants.ODS_USER, Constants.ODS_PWD) + // 使用工具方法拼接Doris连接器参数
                ")"; // 结束建表语句
        tEnv.executeSql(TB_UPGRADE_TICKET); // 执行建表SQL

        // 2. 从ODS表中查询数据（按ETL_DATE分区过滤）
        String query = "SELECT \n" +
                "\tCONCAT(o.ORDER_NO, ut.NUMBERS) AS PK_ID,\n" +
                "\to.SELL_DATE,\n" +
                "\to.CHANNEL,\n" +
                "\to.ORDER_NO,\n" +
                "\to.MOBILE,\n" +
                "\tut.NUMBERS,\n" +
                "\tut.AMOUNT,\n" +
                "\tut.STATUS,\n" +
                "\tut.USETIME\n" +
                "FROM T_ODS_JSSC_TB_OTA_ORDER o\n" +
                "INNER JOIN T_ODS_JSSC_TB_UPGRADE_TICKET ut ON\n" +
                "\to.ID = ut.ORDER_ID\n" +
                "WHERE o.STATUS = '2'\n" +
                "\tAND o.SELL_DATE >= CAST('" + etlDate + "' AS TIMESTAMP) - INTERVAL '365' DAY(3)\n" +
                "\tAND o.SELL_DATE <= CAST('" + etlDate + "' AS TIMESTAMP)"; // 基于SELLDATE过滤
        Table sourceTable = tEnv.sqlQuery(query); // 执行SQL，得到Table

        // 3. 将Table转换为DataStream<Row>
        DataStream<Row> sourceStream = tEnv.toDataStream(sourceTable); // 将Table转换为DataStream<Row>

        // 4. Row映射为UpgradeCouponSalesFactModel实体流
        SingleOutputStreamOperator<UpgradeCouponSalesFactModel> jsscOtaOrderStream = sourceStream // 基于源数据流创建实体映射流
                .map(row -> { // 对每条Row进行映射
                    UpgradeCouponSalesFactModel model = new UpgradeCouponSalesFactModel(); // 创建实体对象

                    String pkId = row.getFieldAs("PK_ID"); // 以订单号作为主键
                    if (pkId == null) { // 判断主键是否为空
                        System.out.println("PK_ID为空" + row.toString()); // 打印告警日志
                        return null; // 主键为空则丢弃该记录
                    } // if结束
                    model.setPkId(pkId); // 设置主键ID

                    // 销售日期和时间处理
                    Object sellDateObj = row.getFieldAs("SELL_DATE"); // 获取销售时间对象
                    if (sellDateObj != null) { // 判断对象是否非空
                        String sellDateStr;
                        if (sellDateObj instanceof LocalDateTime) {
                            // 如果是LocalDateTime类型，转换为字符串
                            sellDateStr = ((LocalDateTime) sellDateObj).toString();
                        } else {
                            // 如果已经是字符串类型
                            sellDateStr = sellDateObj.toString();
                        }
                        if (StringUtils.isNotBlank(sellDateStr)) { // 判断字符串是否非空
                            String salesDate = null;
                            String salesTime = null;

                            if (sellDateStr.contains("T")) {
                                // 处理 ISO 格式: 2016-07-30T11:20:23 或 2016-07-30T00:00
                                String[] parts = sellDateStr.split("T");
                                salesDate = parts[0];
                                salesTime = parts.length > 1 ? parts[1] : "00:00:00";
                                // 确保时间格式为 HH:mm:ss
                                if (salesTime.length() == 5) { // 只有 HH:mm 的情况
                                    salesTime += ":00";
                                }
                            } else if (sellDateStr.contains(" ")) {
                                // 处理空格分隔格式: 2016-07-30 11:20:23 或 2016-07-30 00:00
                                String[] parts = sellDateStr.split(" ");
                                salesDate = parts[0];
                                salesTime = parts.length > 1 ? parts[1] : "00:00:00";
                                // 确保时间格式为 HH:mm:ss
                                if (salesTime.length() == 5) { // 只有 HH:mm 的情况
                                    salesTime += ":00";
                                }
                            }

                            if (salesDate != null) {
                                model.setSalesDate(salesDate); // 设置销售日期
                                model.setSalesTime(salesTime); // 设置销售时间
                            }
                        }
                    }
                    model.setSalesChannel(NormalizationUtils.standardize(FieldType.UPGRADE_SALES_CHANNEL, row.getFieldAs("CHANNEL"))); // 设置销售渠道
                    model.setSalesOrderNumber(row.getFieldAs("ORDER_NO")); // 设置销售订单号

                    // 购买人TID和源ID处理
                    String mobile = row.getFieldAs("MOBILE");
                    if (StringUtils.isNotBlank(mobile)) {
                        Tid tid = new Tid();
                        tid.setMobilePhone(mobile);
                        String buyerTid = IdMapping.idMappingFunction(tid, "JSSC");
                        model.setBuyerTid(buyerTid); // 设置购买人TID
                    }
                    model.setBuyerSourceId(mobile); // 设置购买人源ID为手机号

                    model.setCouponCode(row.getFieldAs("NUMBERS")); // 设置券码

                    // 券面额处理
                    Object amountObj = row.getFieldAs("AMOUNT"); // 获取券面额对象
                    if (amountObj != null) { // 判断对象是否非空
                        String amountStr = amountObj.toString(); // 转换为字符串
                        if (StringUtils.isNotBlank(amountStr)) { // 判断字符串是否非空
                            try {
                                model.setCouponValue(new java.math.BigDecimal(amountStr)); // 转换为BigDecimal类型
                            } catch (NumberFormatException e) { // 捕获数字格式异常
                                System.out.println("券面额格式错误: " + amountStr); // 打印错误日志
                            }
                        }
                    }

                    // 券状态处理
                    Object statusObj = row.getFieldAs("STATUS");
                    String statusStr = statusObj != null ? statusObj.toString() : null;
                    model.setCouponStatus(statusStr); // 设置券状态

                    // 券使用日期和时间处理
                    Object useTimeObj = row.getFieldAs("USETIME"); // 获取券使用时间对象
                    if (useTimeObj != null) { // 判断对象是否非空
                        String useTimeStr;
                        if (useTimeObj instanceof LocalDateTime) {
                            // 如果是LocalDateTime类型，转换为字符串
                            useTimeStr = ((LocalDateTime) useTimeObj).toString();
                        } else {
                            // 如果已经是字符串类型
                            useTimeStr = useTimeObj.toString();
                        }
                        if (StringUtils.isNotBlank(useTimeStr)) { // 判断字符串是否非空
                            String useDate = null;
                            String useTime = null;
                            if (useTimeStr.contains("T")) {
                                // 处理 ISO 格式: 2016-07-30T11:20:23 或 2016-07-30T00:00
                                String[] parts = useTimeStr.split("T");
                                useDate = parts[0];
                                useTime = parts.length > 1 ? parts[1] : "00:00:00";
                                // 确保时间格式为 HH:mm:ss
                                if (useTime.length() == 5) { // 只有 HH:mm 的情况
                                    useTime += ":00";
                                }
                            } else if (useTimeStr.contains(" ")) {
                                // 处理空格分隔格式: 2016-07-30 11:20:23 或 2016-07-30 00:00
                                String[] parts = useTimeStr.split(" ");
                                useDate = parts[0];
                                useTime = parts.length > 1 ? parts[1] : "00:00:00";
                                // 确保时间格式为 HH:mm:ss
                                if (useTime.length() == 5) { // 只有 HH:mm 的情况
                                    useTime += ":00";
                                }
                            }
                            if (useDate != null) {
                                model.setCouponUseDate(useDate); // 设置券使用日期
                                model.setCouponUseTime(useTime); // 设置券使用时间
                            }
                        }
                    }
                    
                    String now = LocalDateTime.now().toString(); // 获取当前系统时间字符串
                    model.setSystemCreatetime(now); // 设置系统创建时间
                    model.setSystemLastUpdatetime(now); // 设置系统最后更新时间

                    return model; // 返回映射后的实体对象
                }) // map结束
                .filter(Objects::nonNull); // 过滤掉空对象（主键为空的记录）

        // 5. 创建Doris Sink并写入DWD表
        DorisSink<UpgradeCouponSalesFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_UPGRADE_COUPON_SALES_FACT"); // 创建Doris Sink，目标DWD表
        jsscOtaOrderStream.print("jsscOtaOrderStream");
        jsscOtaOrderStream.sinkTo(dorisSink); // 将实体流写入Doris

        // 6. 启动作业
        env.execute("JsscTbOtaOrderToDwd"); // 启动作业并命名
    }
}
