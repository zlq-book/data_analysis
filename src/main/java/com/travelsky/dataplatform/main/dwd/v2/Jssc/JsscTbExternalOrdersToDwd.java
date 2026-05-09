//package com.travelsky.dataplatform.main.dwd.v2.Jssc;
//
//import com.travelsky.dataplatform.constans.Constants;
//import com.travelsky.dataplatform.constans.trp.utils.DateUtil;
//import com.travelsky.dataplatform.utils.*;
//import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
//import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.UpgradeInflightBusinessFact;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.doris.flink.sink.DorisSink;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.types.Row;
//
//import java.time.LocalDate;
//import java.time.LocalDateTime;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Objects;
//
//import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.getCabinClass;
//
///**
// * 基于ODS的JSSC外部订单(机上升舱)抽取至 升舱-机上升舱-业务事实表 DWD层
// * UPGRADE_TIME是T到T-30天的数据，只取CHANNEL_TYPE=2，UPGRADE_STATUS=1（成功），PAY_STATUS等于2或7的的数据进入本表*
// */
//public class JsscTbExternalOrdersToDwd {
//
//    public static void main(String[] args) throws Exception {
//        if (args.length < 1) { // 判断入参长度是否小于1
//            System.exit(0); // 没有传入etl_date参数则退出程序
//        } // if结束
//        String etlDate = args[0]; // 获取etl_date参数
//        // 将字符串转换为 LocalDate
//        LocalDate currentDate = LocalDate.parse(etlDate);
//        // 计算30前的日期
//        LocalDate localDate = currentDate.minusDays(30);
//        // 格式化为字符串
//        String s1 = localDate.toString();
//        String startDate = s1;
//        String endDate = etlDate;
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); // 创建Flink流执行环境
//        CheckpointUtils.setCheckpoint(env, "JsscTbExternalOrdersToDwd"); // 设置Checkpoint，便于任务容错与重启
//        env.setParallelism(1); // 设置并行度为1，便于调试与串行输出
//        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env); // 创建Table环境
//        // 1. 创建Doris目标表
//        String TB_EXTERNAL_ORDERS = "CREATE TABLE T_ODS_JSSC_TB_EXTERNAL_ORDERS (\n" + // 构建T_ODS_JSSC_TB_EXTERNAL_ORDERS建表语句开头
//                "    UPGRADE_ORDER_ID VARCHAR(90),\n" + // 升舱订单id
//                "    ORDER_NO         VARCHAR(90),\n" + // 升舱订单号
//                "    FLT_DATE         VARCHAR(90),\n" + // 航班日期
//                "    FLT_NUM          VARCHAR(90),\n" + // 航班号
//                "    ORIG             VARCHAR(90),\n" + // 出发地
//                "    DEST             VARCHAR(90),\n" + // 目的地
//                "    TICKET_NUM       VARCHAR(90),\n" + // 旅客票号
//                "    UPGRADE_PRICE    VARCHAR(90),\n" + // 升舱金额
//                "    BEFORE_CABIN     VARCHAR(90),\n" + // 升舱前舱位
//                "    BEFORE_SEAT      VARCHAR(90),\n" + // 升舱前座位号
//                "    AFTER_CABIN      VARCHAR(90),\n" + // 升舱后舱位
//                "    PSGNAME          VARCHAR(50),\n" + // 旅客姓名
//                "    FLY_MILEAGE      VARCHAR(90),\n" + // 升舱航段航距
//                "    USER_ID          VARCHAR(90),\n" + // 移动客舱登录账号
//                "    ACCT             VARCHAR(90),\n" + // 移动客舱登录账号对应工号
//                "    TAKE_OFF         VARCHAR(100),\n" + // 起飞时间
//                "    UPGRADE_TIME     TIMESTAMP(6),\n" + // 升舱订单创建时间
//                "    MOBILE           VARCHAR(90),\n" + // 旅客手机号
//                "    UPGRADE_STATUS   VARCHAR(90),\n" + // 升舱状态
//                "    OPERATOR         VARCHAR(90),\n" + // 员工工号（操作人）办理人
//                "    CABIN_CREW_NAME  VARCHAR(120),\n" + // 姓名(空乘姓名)
//                "    PAY_TYPE         VARCHAR(100),\n" + // 支付方式
//                "    UPGRADE_ORDER_NO VARCHAR(90),\n" + // 支付订单号
//                "    COUON_NO         VARCHAR(90),\n" + // 使用的升舱卡券号
//                "    PAY_PRICE        VARCHAR(90),\n" + // 支付金额
//                "    PAY_STATUS       VARCHAR(90),\n" + // 支付状态
//                "    PAY_TIME         VARCHAR(90),\n" + // 支付时间
//                "    REMARK           VARCHAR(800),\n" + // 备注
//                "    INVOICE_STATE    VARCHAR(90),\n" + // 发票邮寄状态
//                "    TRADE_NO         VARCHAR(90),\n" + // 支付平台交易号
//                "    PRODUCT_NO       VARCHAR(90),\n" + // 产品编号
//                "    PRODUCT_NAME     VARCHAR(180),\n" + // 产品名称
//                "    AFTER_SEAT       VARCHAR(90),\n" + // 升舱后座位
//                "    PSG_CARDID       VARCHAR(90),\n" + // 旅客证件号
//                "    SALE_MAN         VARCHAR(90),\n" + // 销售人
//                "    UPGFADED_TIME    TIMESTAMP(6),\n" + // 升舱时间
//                "    EMD_TICKET       VARCHAR(90),\n" + // EMD票号
//                "    CHANNEL_TYPE     VARCHAR(90),\n" + // 接口渠道类型
//                "    DPI_IDX          VARCHAR(90),\n" + // 经停航班统一标识
//                "    PAYCHANNEL       VARCHAR(90),\n" + // 支付渠道
//                "    MAKEUPFLAG       VARCHAR(90),\n" + // 补差标识
//                "    CARDUSEDNUM      VARCHAR(90),\n" + // 卡当时使用次数
//                "    CARDAVAILNUM     VARCHAR(90),\n" + // 卡当时剩余次数
//                "    ORDER_STATUS     VARCHAR(90),\n" + // 订单状态
//                "    IS_POST          VARCHAR(90),\n" + // 是否邮寄
//                "    SERIAL_NO        VARCHAR(100),\n" + // 发票本地流水号
//                "    FLY_TYPE         VARCHAR(90),\n" + // 航线类型
//                "    CREATE_TIME      VARCHAR(90),\n" + // 机上升舱--升舱订单入库时间
//                "    ETL_DATE         DATE\n" + // 数据ETL日期
//                ") WITH (\n" + // 结束列定义并开始WITH子句
//                GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_JSSC_TB_EXTERNAL_ORDERS", Constants.ODS_USER, Constants.ODS_PWD) + // 使用工具方法拼接Doris连接器参数
//                ")"; // 结束建表语句
//        tEnv.executeSql(TB_EXTERNAL_ORDERS); // 执行建表SQL
//        // 1. 从ODS表中查询数据（按ETL_DATE分区过滤）
//        String query = "SELECT \n" + // 构造SQL查询字符串，选择需要的字段
//                "ETL_DATE,\n" + // 选择ETL_DATE字段
//                "ORDER_NO,\n" + // 升舱订单号
//                "FLT_DATE,\n" + // 航班日期
//                "FLT_NUM,\n" + // 航班号
//                "ORIG,\n" + // 出发地
//                "DEST,\n" + // 目的地
//                "TICKET_NUM,\n" + // 旅客票号
//                "UPGRADE_PRICE,\n" + // 升舱金额
//                "BEFORE_CABIN,\n" + // 升舱前舱位
//                "BEFORE_SEAT,\n" + // 升舱前座位号
//                "AFTER_CABIN,\n" + // 升舱后舱位
//                "PSGNAME,\n" + // 旅客姓名
//                "FLY_MILEAGE,\n" + // 升舱航段航距
//                "USER_ID,\n" + // 移动客舱登录账号
//                "ACCT,\n" + // 移动客舱登录账号对应工号
//                "TAKE_OFF,\n" + // 起飞时间
//                "UPGRADE_TIME,\n" + // 升舱订单创建时间
//                "MOBILE,\n" + // 旅客手机号
//                "UPGRADE_STATUS,\n" + // 升舱状态
//                "OPERATOR,\n" + // 办理人
//                "CABIN_CREW_NAME,\n" + // 空乘姓名
//                "PAY_TYPE,\n" + // 支付方式
//                "UPGRADE_ORDER_NO,\n" + // 支付订单号
//                "COUON_NO,\n" + // 升舱卡券号
//                "PAY_PRICE,\n" + // 支付金额
//                "PAY_STATUS,\n" + // 支付状态
//                "PAY_TIME,\n" + // 支付时间
//                "REMARK,\n" + // 备注
//                "INVOICE_STATE,\n" + // 发票邮寄状态
//                "TRADE_NO,\n" + // 支付平台交易号
//                "PRODUCT_NO,\n" + // 产品编号
//                "PRODUCT_NAME,\n" + // 产品名称
//                "AFTER_SEAT,\n" + // 升舱后座位
//                "PSG_CARDID,\n" + // 旅客证件号
//                "SALE_MAN,\n" + // 销售人
//                "UPGFADED_TIME,\n" + // 升舱时间
//                "EMD_TICKET,\n" + // EMD票号
//                "CHANNEL_TYPE,\n" + // 接口渠道类型
//                "DPI_IDX,\n" + // 经停航班统一标识
//                "PAYCHANNEL,\n" + // 支付渠道
//                "MAKEUPFLAG,\n" + // 补差标识
//                "CARDUSEDNUM,\n" + // 卡当时使用次数
//                "CARDAVAILNUM,\n" + // 卡当时剩余次数
//                "ORDER_STATUS,\n" + // 订单状态
//                "IS_POST,\n" + // 是否邮寄
//                "SERIAL_NO,\n" + // 发票本地流水号
//                "FLY_TYPE,\n" + // 航线类型
//                "CREATE_TIME\n" + // 升舱订单入库时间
//                "FROM T_ODS_JSSC_TB_EXTERNAL_ORDERS \n"  // 查询的ODS表
//                + " WHERE UPGRADE_TIME >= '" + startDate + " 00:00:00' AND UPGRADE_TIME <= '" + endDate + " 23:59:59' "
//                + " AND CHANNEL_TYPE = 2 AND UPGRADE_STATUS = 1 "
//                + " AND PAY_STATUS in (2, 7)"
//                ;
//        Table sourceTable = tEnv.sqlQuery(query); // 执行SQL，得到Table
//
//        // 2. 将Table转换为DataStream<Row>
//        DataStream<Row> sourceStream = tEnv.toDataStream(sourceTable); // 将Table转换为DataStream<Row>
//
//        // 3. Row映射为UpgradeInflightBusinessFact实体流
//        SingleOutputStreamOperator<UpgradeInflightBusinessFact> factStream = sourceStream // 基于源数据流创建实体映射流
//                .map(row -> { // 对每条Row进行映射
//                    UpgradeInflightBusinessFact model = new UpgradeInflightBusinessFact(); // 创建实体对象
//
//                    String pkId = row.getFieldAs("ORDER_NO"); // 以订单号作为主键
//                    if (pkId == null) { // 判断主键是否为空
//                        System.out.println("ORDER_NO为空" +  row.toString()); // 打印告警日志
//                        return null; // 主键为空则丢弃该记录
//                    } // if结束
//                    model.setPkId(pkId); // 设置主键ID
//
//                    model.setOrderNo(row.getFieldAs("ORDER_NO")); // 设置升舱订单号
//
//                    String upgradeTimeStr = row.getFieldAs("UPGRADE_TIME") !=null ? row.getFieldAs("UPGRADE_TIME").toString() : ""; // 获取升舱时间原始字符串
//                    if (StringUtils.isNotBlank(upgradeTimeStr)) {// 判断字符串是否非空
//                        String s = DateUtil.isoToTraditional(upgradeTimeStr);
//                        Date parsed = DateUtil.dateParse(s, DateUtil.DATE_FORMAT); // 按yyyy-MM-dd HH:mm:ss解析为Date
//                        if (parsed != null) {
//                            String upgradeDate = DateUtil.formatDate(parsed, DateUtil.DATE_PATTERN); // 格式化为yyyy-MM-dd日期
//                            String upgradeTime = DateUtil.formatDate(parsed, "HH:mm:ss"); // 格式化为HH:mm:ss时间
//                            model.setUpgradeDate(upgradeDate); // 设置升舱申请日期
//                            model.setUpgradeTime(upgradeTime); // 设置升舱申请时间
//                        }
//                    }
//
//                    model.setOrig(row.getFieldAs("ORIG")); // 设置起飞机场
//                    model.setDest(row.getFieldAs("DEST")); // 设置到达机场
//                    String fltDate = row.getFieldAs("FLT_DATE");
//                    model.setFltDate(fltDate); // 设置航班日期
//                    model.setTakeOff(row.getFieldAs("TAKE_OFF")); // 设置航班时间
//                    model.setPsgname(row.getFieldAs("PSGNAME")); // 设置乘机人姓名
//                    model.setPsgCardid(row.getFieldAs("PSG_CARDID")); // 设置乘机人证件号
//                    // 源数据：Z-国内，W-国际（有为空的可能），需要标准化：D - 国内  I - 国际
//                    String flyType = row.getFieldAs("FLY_TYPE");
//                    if ("W".equals(flyType)) flyType = "I";
//                    if ("Z".equals(flyType)) flyType = "D";
//                    model.setFlyType(flyType); // 设置国内国际标识
//                    model.setTicketNum(row.getFieldAs("TICKET_NUM")); // 设置关联票号
//                    String beforeCabin = row.getFieldAs("BEFORE_CABIN");
//                    model.setBeforeCabin(beforeCabin); // 设置升舱前舱位
//                    //按照舱位舱等规则转化
//                    String beforeCabinClass = getCabinClass(fltDate, beforeCabin);
//                    model.setBeforeFare(beforeCabinClass); // 设置升舱前舱等（无独立字段，使用舱位代填）
//
//                    String afterCabin = row.getFieldAs("AFTER_CABIN");
//                    model.setAfterCabin(afterCabin); // 设置升舱后舱位
//                    //按照舱位舱等规则转化
//                    String afterCabinClass = getCabinClass(fltDate, afterCabin);
//                    model.setAfterFare(afterCabinClass); // 设置升舱后舱等（无独立字段，使用舱位代填）
//
//                    String payType = row.getFieldAs("PAY_TYPE");
//                    model.setPayType(payType); // 设置支付方式
//                    model.setUpgradePrice(row.getFieldAs("UPGRADE_PRICE")); // 设置升舱金额
//
//                    String makeupflag = row.getFieldAs("MAKEUPFLAG");
//                    // 需要将1转成0，对于0转成1，对于空，转成1
//                    makeupflag = StringUtils.isBlank(makeupflag) ? "1" : makeupflag;
//                    makeupflag = "1".equals(makeupflag) ? "0" : makeupflag;
//                    model.setMakeupflag(makeupflag); // 设置升舱服务出票数量/补差标识
//                    //如果PAY_TYPE=3或4或5，写入：是，否则为否
//                    model.setUpgradeCouponUsed("3".equals(payType) || "4".equals(payType) || "5".equals(payType));
//                    // 乘机人TID/预订人TID暂无直接来源，如需可在此处接入映射服务
//                    // 获取tid
//                    String psgCardid = row.getFieldAs("PSG_CARDID");
//                    Tid tid = new Tid();
//                    tid.setTid(psgCardid);
//                    //Map<String, String> certification = new HashMap<>();
//                    //certification.put(credentialType, psgCardid);
//                    //tid.setCertification(certification);
//                    String tidStr = IdMapping.idMappingFunction(tid, "JSSC");
//
//                    model.setFkPassengerUserTid(tidStr); // 设置乘机人TID（暂无映射，置空）
//                    model.setFkBookingUserTid(tidStr); // 设置预订人TID（暂无映射，置空）
//
//                    // 系统审计字段
//                    String now = LocalDateTime.now().toString(); // 获取当前系统时间字符串
//                    model.setSystemCreatetime(now); // 设置系统创建时间
//                    model.setSystemLastUpdatetime(now); // 设置系统最后更新时间
//
//                    return model; // 返回映射后的实体对象
//                }) // map结束
//                .filter(Objects::nonNull); // 过滤掉空对象（主键为空的记录）
//
//        // 4. 创建Doris Sink并写入DWD表
//        DorisSink<UpgradeInflightBusinessFact> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_UPGRADE_INFLIGHT_BUSINESS_FACT"); // 创建Doris Sink，目标DWD表
//        factStream.sinkTo(dorisSink); // 将实体流写入Doris
//
//        // 5. 启动作业
//        env.execute("JsscTbExternalOrdersToDwd"); // 启动作业并命名
//    }
//}
