package com.travelsky.dataplatform.main.dwd.v2.djksc;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.UpgrRefundFactModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.UpgrTikFactModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;
import java.util.Objects;

public class DjkscOrderInfoToDwd {
    public static void main(String[] args) throws Exception { // 主函数入口，抛出异常
        if (args.length < 1) { // 判断入参长度是否小于1
            System.exit(0); // 没有传入etl_date参数则退出程序
        } // if结束
        String etlDate = args[0]; // 获取etl_date参数

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); // 创建Flink流执行环境
        CheckpointUtils.setCheckpoint(env, "DjkscTbOrderInfoToDwd"); // 设置Checkpoint，便于任务容错与重启
        env.setParallelism(1); // 设置并行度为1，便于调试与串行输出
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env); // 创建Table环境

        // 1. 创建Doris目标表
        String T_ODS_DJKSC_ORDER_INFO = "CREATE TABLE T_ODS_DJKSC_ORDER_INFO (\n" +
                "  `ID` STRING COMMENT '主键',\n" +
                "  `ORDER_NO` STRING COMMENT '订单id',\n" +
                "  `USER_ID` STRING COMMENT '行程id',\n" +
                "  `ORDER_STATE` STRING COMMENT '订单状态 0-已占座；2-已升舱；3-已取消；4-升舱失败；5-退款申请中 6-退款成功 7-预约成功',\n" +
                "  `PAY_STATE` STRING COMMENT '订单支付状态0-未支付；1-已支付;',\n" +
                "  `SEAT_NO` STRING COMMENT '升舱座位号',\n" +
                "  `AMOUNT` DECIMAL(24, 2) COMMENT '升舱价格',\n" +
                "  `CLASS_CODE` STRING COMMENT '升舱舱位',\n" +
                "  `EMD_NO` STRING COMMENT '升舱票号',\n" +
                "  `CONTRACT_NAME` STRING COMMENT '联系人姓名',\n" +
                "  `CONTRACT_PHONE` STRING COMMENT '联系人手机',\n" +
                "  `CREATE_TIME` TIMESTAMP(3) COMMENT '订单创建时间',\n" +
                "  `PSR_NAME` STRING COMMENT '旅客姓名',\n" +
                "  `CARD_NO` STRING COMMENT '证件号码',\n" +
                "  `ADDRESS` STRING COMMENT '发票邮寄地址',\n" +
                "  `MODIFY_TIME` TIMESTAMP(3) COMMENT '最近一次订单状态变化的时刻',\n" +
                "  `CHANNEL_CODE` STRING COMMENT '渠道code',\n" +
                "  `INVOICE_STATUS` STRING COMMENT '开票状态 0未开；1已开',\n" +
                "  `REFERRER` STRING COMMENT '推荐人工号',\n" +
                "  `HIGH_CLASS_REWARD` DECIMAL(24, 0) COMMENT '现金升高经舱奖励',\n" +
                "  `BUSINESS_CLASS_REWARD` DECIMAL(24, 0) COMMENT '现金升公务舱舱奖励',\n" +
                "  `BUSINESS_CLASS_VOUCHER__REWARD` DECIMAL(24, 0) COMMENT '升舱券升公务舱舱奖励',\n" +
                "  `BUSINESS_CLASS_CARD_REWARD` DECIMAL(24, 0) COMMENT '升舱卡升公务舱舱奖励',\n" +
                "  `REFUND_REMARK` STRING COMMENT '退款原因',\n" +
                "  `AUDIT_STATUS` STRING COMMENT '退单审核状态 0-待审核 1-一审通过 2-一审拒绝 3-二审通过 4-二审拒绝',\n" +
                "  `ENABLE` STRING COMMENT '数据是否可用：0-不可用 1-可用',\n" +
                "  `CREATE_USER` STRING COMMENT '创建人',\n" +
                "  `UPDATE_TIME` TIMESTAMP(3) COMMENT '修改时间',\n" +
                "  `UPDATE_USER` STRING COMMENT '修改人',\n" +
                "  `UPGRADE_TYPE` STRING COMMENT '升舱方式 0办理升舱；1预约升舱；',\n" +
                "  `UP_ORDER_NO` STRING COMMENT '付费升舱订单号',\n" +
                "  `REMARK` STRING COMMENT '备注，用于保存升舱订单号',\n" +
                "  `SOURCE` STRING COMMENT '来源  ZSF 掌上飞；WX 微信公众号；MNWX 微信小程序；ZSSH 掌上山航；SHARE 员工分享',\n" +
                "  `REFUND_USER` STRING COMMENT '申请退单人(工号)',\n" +
                "  `ORDER_ATTACHMENT_URL` STRING COMMENT '申请退单附件',\n" +
                "  `ETL_DATE` DATE COMMENT '数据ETL日期'\n" +
                ") WITH (\n" + // 结束列定义并开始WITH子句
                GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_DJKSC_ORDER_INFO", Constants.ODS_USER, Constants.ODS_PWD) + // 使用工具方法拼接Doris连接器参数
                ")"; // 结束建表语句
        tEnv.executeSql(T_ODS_DJKSC_ORDER_INFO); // 执行建表SQL
        //创建Doris目标表
        String T_ODS_DJKSC_PAYMENT_INFO = "CREATE TABLE T_ODS_DJKSC_PAYMENT_INFO (\n" +
                "  `PAYMENT_ID` STRING COMMENT 'ID',\n" +
                "  `ORDER_NO` STRING COMMENT '升舱订单号',\n" +
                "  `BILL_NO` STRING COMMENT '支付流水号',\n" +
                "  `BUS_NO` STRING COMMENT '第三方回写支付号',\n" +
                "  `PAY_STATE` STRING COMMENT '支付状态0：未支付；1支付中；2：支付成功；3：支付失败',\n" +
                "  `PAY_TIME` TIMESTAMP(3) COMMENT '支付时间',\n" +
                "  `RETURN_TIME` TIMESTAMP(3) COMMENT '支付回写时间',\n" +
                "  `PAY_WAY` STRING COMMENT '支付方式暂时存升舱券号或升舱卡号',\n" +
                "  `PAY_TYPE` STRING COMMENT '支付方式0现金1升舱券；2免费升舱券；3升舱卡；4支付宝；5微信；6银联；7云闪付',\n" +
                "  `BANK_TYPE` STRING COMMENT '银行名称',\n" +
                "  `BANK_ID` STRING COMMENT '银行id',\n" +
                "  `PAY_MSG` STRING COMMENT '支付回写信息',\n" +
                "  `PAY_AMOUNT` DECIMAL(24, 2) COMMENT '支付金额',\n" +
                "  `RF_NO` STRING COMMENT '退款申请号',\n" +
                "  `REFUND_STATE` STRING COMMENT '退款状态0：退款中，1：退款成功，2：退款失败',\n" +
                "  `CHANNEL_CODE` STRING COMMENT '渠道CODE',\n" +
                "  `BANK_ORDER_NO` STRING COMMENT '银行订单号',\n" +
                "  `ENABLE` STRING COMMENT '数据是否可用：0-不可用1-可用',\n" +
                "  `CREATE_TIME` TIMESTAMP(3) COMMENT '创建时间',\n" +
                "  `CREATE_USER` STRING COMMENT '创建人',\n" +
                "  `UPDATE_TIME` TIMESTAMP(3) COMMENT '修改时间',\n" +
                "  `UPDATE_USER` STRING COMMENT '修改人',\n" +
                "  `REFUND_TIME` TIMESTAMP(3) COMMENT '退款时间',\n" +
                "  `OUT_RF_NO` STRING COMMENT '第三方支付平台退款编号',\n" +
                "  `ETL_DATE` DATE COMMENT '数据ETL日期'\n" + // 使用时间
                ") WITH (\n" + // 结束列定义并开始WITH子句
                GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_DJKSC_PAYMENT_INFO", Constants.ODS_USER, Constants.ODS_PWD) + // 使用工具方法拼接Doris连接器参数
                ")"; // 结束建表语句
        tEnv.executeSql(T_ODS_DJKSC_PAYMENT_INFO); // 执行建表SQL
        //创建Doris目标表
        String T_ODS_DJKSC_UPGFLT_ORDER_INFO = "CREATE TABLE T_ODS_DJKSC_UPGFLT_ORDER_INFO (\n" +
                "  `USER_ID` STRING COMMENT 'ID',\n" +
                "  `FLIGHT_NO` STRING COMMENT '航班号',\n" +
                "  `FLIGHT_DATE` TIMESTAMP(3) COMMENT '航班日期',\n" +
                "  `FLIGHT_TIME` STRING COMMENT '航班起飞时间',\n" +
                "  `ORGCITY_NAME` STRING COMMENT '出发城市名称'," +
                "  `DSTCITY_NAME` STRING COMMENT '到达城市名称',\n" +
                "  `UPGRADE_BEGIN_TIME` STRING COMMENT '升舱业务办理开始时间',\n" +
                "  `UPGRADE_END_TIME` STRING COMMENT '升舱业务办理结束时间',\n" +
                "  `PSR_NAME` STRING COMMENT '旅客姓名',\n" +
                "  `CARD_NO` STRING COMMENT '证件号码',\n" +
                "  `CARD_TYPE` STRING COMMENT '证件类型',\n" +
                "  `CONTACT_INFO` STRING COMMENT '联系方式',\n" +
                "  `PSR_LEVEL` STRING COMMENT '常客等级',\n" +
                "  `FFP_NO` STRING COMMENT '常客卡号',\n" +
                "  `PNR_NO` STRING COMMENT 'PNR号',\n" +
                "  `TICKET_NO` STRING COMMENT '票号',\n" +
                "  `TOUR_INDEX` STRING COMMENT '序号',\n" +
                "  `CURRENT_CLASS` STRING COMMENT '当前舱位',\n" +
                "  `CURRENT_SEAT` STRING COMMENT '当前座位号',\n" +
                "  `UPCLASS_INFO` STRING COMMENT '可升舱的舱位以及价格信息如：F,300;C,200;',\n" +
                "  `ERROR_REASON` STRING COMMENT '不可升舱原因',\n" +
                "  `ERROR_CODE` STRING COMMENT '不可升舱错误码',\n" +
                "  `IS_PUSH` STRING COMMENT '是否推送0：是；1否',\n" +
                "  `CREATE_DATE` TIMESTAMP(3) COMMENT '创建时间',\n" +
                "  `ORGCITY_CODE` STRING COMMENT '出发机场三字码',\n" +
                "  `DSTCITY_CODE` STRING COMMENT '到达机场三字码',\n" +
                "  `ENABLE` STRING COMMENT '数据是否可用：0-不可用 1-可用',\n" +
                "  `CREATE_TIME` TIMESTAMP(3) COMMENT '创建时间',\n" +
                "  `CREATE_USER` STRING COMMENT '创建人',\n" +
                "  `UPDATE_TIME` TIMESTAMP(3) COMMENT '修改时间',\n" +
                "  `UPDATE_USER` STRING COMMENT '修改人',\n" +
                "  `ORG_AIRPORT_NAME` STRING COMMENT '出发机场名称',\n" +
                "  `DST_AIRPORT_NAME` STRING COMMENT '到达机场名称',\n" +
                "  `BOARDING_GATE_NUMBER` STRING COMMENT '表示登机口待定',\n" +
                "  `BORDING_TIME` STRING COMMENT '登机时间',\n" +
                "  `ETL_DATE` DATE COMMENT '数据ETL日期'\n" +
                ") WITH (\n" + // 结束列定义并开始WITH子句
                GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_DJKSC_UPGFLT_ORDER_INFO", Constants.ODS_USER, Constants.ODS_PWD) + // 使用工具方法拼接Doris连接器参数
                ")"; // 结束建表语句
        tEnv.executeSql(T_ODS_DJKSC_UPGFLT_ORDER_INFO); // 执行建表SQL

        // 2. 从ODS表中查询数据（按ETL_DATE分区过滤）
        String query = "SELECT\n" +
                " CONCAT(oi.EMD_NO,uoi.ORGCITY_CODE,uoi.DSTCITY_CODE) PK_ID, oi.ORDER_NO, oi.CHANNEL_CODE, oi.ORDER_STATE, pi.PAY_TYPE, pi.PAY_AMOUNT\n" +
                "FROM\n" +
                " T_ODS_DJKSC_ORDER_INFO oi\n" +
                "LEFT JOIN T_ODS_DJKSC_PAYMENT_INFO pi\n" +
                "ON\n" +
                " oi.ORDER_NO = pi.ORDER_NO\n" +
                "LEFT JOIN T_ODS_DJKSC_UPGFLT_ORDER_INFO uoi ON\n" +
                " oi.USER_ID = uoi.USER_ID\n" +
                "WHERE EMD_NO IS NOT NULL\n" +
                " AND ETL_DATE BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;
        Table sourceTable = tEnv.sqlQuery(query); // 执行SQL，得到Table

        // 3. 将Table转换为DataStream<Row>
        DataStream<Row> sourceStream = tEnv.toDataStream(sourceTable); // 将Table转换为DataStream<Row>

        // 4. Row映射为FactModel实体流
        SingleOutputStreamOperator<UpgrTikFactModel> djkscOrderStream = sourceStream // 基于源数据流创建实体映射流
                .map(row -> { // 对每条Row进行映射
                    UpgrTikFactModel model = new UpgrTikFactModel(); // 创建实体对象

                    String pkId = row.getFieldAs("PK_ID"); // 以订单号作为主键
                    model.setPkId(pkId); // 设置主键ID

                    String orderNo = row.getFieldAs("ORDER_NO"); // 以订单号作为主键
                    model.setAkOrdernum(orderNo);

                    //支付方式
                    String payType = row.getFieldAs("PAY_TYPE");
                    String standardizePayType = NormalizationUtils.standardize(FieldType.ORDER_PAYMENT_METHOD, payType, DataSource.GATE_UPGRADE);
                    model.setPayType(standardizePayType);
                    //原币种升舱金额
                    if ("VCH".equals(standardizePayType)) {
                        model.setUpgrdoldAmount(0.0);
                        model.setUseUpgrCoupon(true);
                    } else {
                        Double payAmount = row.getFieldAs("PAY_AMOUNT");
                        model.setUpgrdoldAmount(payAmount);
                        model.setUseUpgrCoupon(false);
                    }
                    //升舱金额CNY
                    //夜维sql语句修正

                    //预订渠道
                    String channelCode = row.getFieldAs("CHANNEL_CODE");
                    String standardizeChannelCode = NormalizationUtils.standardize(FieldType.UPGRADE_ORDER_CHANNEL, channelCode);
                    model.setAkBookChannel(standardizeChannelCode);

                    String now = LocalDateTime.now().toString(); // 获取当前系统时间字符串
                    model.setSystemCreatetime(now); // 设置系统创建时间
                    model.setSystemLastUpdatetime(now); // 设置系统最后更新时间

                    return model; // 返回映射后的实体对象
                }) // map结束
                .filter(Objects::nonNull); // 过滤掉空对象（主键为空的记录）

        // 5. 创建Doris Sink并写入DWD表
        DorisSink<UpgrTikFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_UPGR_TIK_FACT"); // 创建Doris Sink，目标DWD表
        djkscOrderStream.print("djkscOrderStream");
        djkscOrderStream.sinkTo(dorisSink); // 将实体流写入Doris

        // 4.1. Row映射为FactModel实体流
        SingleOutputStreamOperator<UpgrRefundFactModel> djkscOrderRefundStream = sourceStream // 基于源数据流创建实体映射流
                .map(row -> { // 对每条Row进行映射

                    //订单状态
                    String orderState = row.getFieldAs("ORDER_STATE");
                    if ("6".equals(orderState)) {
                        UpgrRefundFactModel model = new UpgrRefundFactModel(); // 创建实体对象
                        String pkId = row.getFieldAs("PK_ID"); // 以订单号作为主键
                        model.setPkId(pkId); // 设置主键ID
                        String orderNo = row.getFieldAs("ORDER_NO"); // 以订单号作为主键
                        model.setAkOrdernum(orderNo);
                        //支付方式
                        String payType = row.getFieldAs("PAY_TYPE");
                        String standardizePayType = NormalizationUtils.standardize(FieldType.ORDER_PAYMENT_METHOD, payType, DataSource.GATE_UPGRADE);
                        model.setPayType(standardizePayType);
                        //原币种升舱金额
                        if ("VCH".equals(standardizePayType)) {
                            model.setUpgrdoldAmount(0.0);
                            model.setUseUpgrCoupon(true);
                        } else {
                            Double payAmount = row.getFieldAs("PAY_AMOUNT");
                            model.setUpgrdoldAmount(payAmount);
                            model.setUseUpgrCoupon(false);
                        }
                        //升舱金额CNY
                        //夜维sql语句修正

                        //预订渠道
                        String channelCode = row.getFieldAs("CHANNEL_CODE");
                        String standardizeChannelCode = NormalizationUtils.standardize(FieldType.ORDER_CHANNEL_AIRTICKET, channelCode);
                        model.setAkRefundChannel(standardizeChannelCode);

                        String now = LocalDateTime.now().toString(); // 获取当前系统时间字符串
                        model.setSystemCreatetime(now); // 设置系统创建时间
                        model.setSystemLastUpdatetime(now); // 设置系统最后更新时间
                        return model; // 返回映射后的实体对象
                    }
                    return null;
                }) // map结束
                .filter(Objects::nonNull); // 过滤掉空对象（主键为空的记录）

        // 5.1. 创建Doris Sink并写入DWD表
        DorisSink<UpgrRefundFactModel> dorisSinktoOrderRefund = FlinkDorisUtils.creatDorisDWDSink("T_DWD_UPGR_REFUND_FACT"); // 创建Doris Sink，目标DWD表
        djkscOrderRefundStream.print("djkscOrderRefundStream");
        djkscOrderRefundStream.sinkTo(dorisSinktoOrderRefund); // 将实体流写入Doris


        // 6. 启动作业
        env.execute("DjkscOrderInfoToDwd"); // 启动作业并命名
    }
}
