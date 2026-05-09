package com.travelsky.dataplatform.main.ods.v2.khbp;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

/**
 * @author
 * @date 2025/9/25 16:29
 */
public class ExtractTOdsKfbpBusFare {
    public static Logger log = LoggerFactory.getLogger(ExtractTOdsKfbpBusFare.class);
    public static void main(String[] args) throws IOException {
        log.info(" ExtractTOdsKfbpBusFare data transfer start");
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String startDate = etlDate;
        String endDate = etlDate;
        if (args.length > 2) {
            startDate = args[1];
            endDate = args[2];
        }
        log.info("ExtractTOdsKfbpBusOrder etl_date:" + etlDate+", startDate:"+startDate+", endDate:"+endDate);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, " ExtractTOdsKfbpBusFare");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建上游ORACLE数据源表
        log.info(" ExtractTOdsKfbpBusFare executeSql create table for source start");
        tEnv.executeSql("CREATE TABLE BUS_FARE (\n" +
                " PKID VARCHAR(255),\n" +
                " CREATE_BY VARCHAR(40),\n" +
                " CREATE_DATE TIMESTAMP(6),\n" +
                " CREATE_NAME VARCHAR(40),\n" +
                " UPDATE_BY VARCHAR(40),\n" +
                " UPDATE_DATE TIMESTAMP(6),\n" +
                " UPDATE_NAME VARCHAR(40),\n" +
                " BIRTH TIMESTAMP(6),\n" +
                " COUNTY VARCHAR(255),\n" +
                " DEP_NAME VARCHAR(255),\n" +
                " EMAIL VARCHAR(128),\n" +
                " FARE_CATEGORY INT,\n" +
                " FARE_NAME VARCHAR(255),\n" +
                " FARE_TYPE INT,\n" +
                " FARE_TYPECN VARCHAR(255),\n" +
                " FFP_CARD_NO VARCHAR(255),\n" +
                " FOOD_CODE VARCHAR(255),\n" +
                " FOOD_TYPE INT,\n" +
                " GENDER INT,\n" +
                " ID_CODE VARCHAR(32),\n" +
                " ID_CODEGP VARCHAR(30),\n" +
                " ID_EXPIRY_DATE TIMESTAMP(6),\n" +
                " ID_TYPE INT,\n" +
                " ID_TYPECN VARCHAR(255),\n" +
                " ID_TYPEGP INT,\n" +
                " IS_SEND_SMS tinyINT,\n" +
                " ISSUE_COUNTRY VARCHAR(255),\n" +
                " MOBILE VARCHAR(255),\n" +
                " PARENT_NAME VARCHAR(255),\n" +
                " PRICE_ALL_FEE DECIMAL(19, 2),\n" +
                " PRICE_BUILD_FEE DECIMAL(19, 2),\n" +
                " PRICE_CABIN_PRICE DECIMAL(19, 2),\n" +
                " PRICE_CURRENCY INT,\n" +
                " PRICE_EXCHANGE_RATE DECIMAL(10,4),\n" +
                " PRICE_FUEL_FEE DECIMAL(19, 2),\n" +
                " PRICE_OB_FEE DECIMAL(19, 2),\n" +
                " PRICE_TICKET_PRICE DECIMAL(19, 2),\n" +
                " PRICE_TOTAL_PRICE DECIMAL(19, 2),\n" +
                " SPECIAL_TYPE INT,\n" +
                " TAXES_JSON VARCHAR(1000),\n" +
                " US_ADDRESS VARCHAR(100),\n" +
                " US_CITY VARCHAR(40),\n" +
                " ZHIWU VARCHAR(128),\n" +
                " ZIP_CODE VARCHAR(20),\n" +
                " BUS_ORDER_PKID VARCHAR(255),\n" +
                " INSURE_ORDER_PKID VARCHAR(255),\n" +
                " ORDER_PKID VARCHAR(255),\n" +
                " INDEX_IN_PNR VARCHAR(3),\n" +
                " FARE_NAME_CN VARCHAR(255),\n" +
                " GOV_BIN VARCHAR(255),\n" +
                " GOV_BUGDET_NAME VARCHAR(255),\n" +
                " FARE_NAMEEN VARCHAR(255),\n" +
                " GMJC_RMK VARCHAR(64),\n" +
                " LYX_ORDERS VARCHAR(255)" +

                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.KHBP_IP + ":" + Constants.KHBP_PORT + "/" + Constants.KHBP_DB + "',\n" +
                "    'table-name' = '" + Constants.KHBP_SCHEMA + ".BUS_FARE', \n" +
                "    'username' = '" + Constants.KHBP_USER + "',\n" +
                "    'password' = '" + Constants.KHBP_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        log.info(" ExtractTOdsKfbpBusFare executeSql create table for source end");
        //创建Doris目标表
        log.info(" ExtractTOdsKfbpBusFare executeSql create table for doris start");
        tEnv.executeSql("CREATE TABLE `T_ODS_KFBP_BUS_FARE` (\n" +
                "  `PKID` varchar(1000) NOT NULL COMMENT '主键',\n" +
                "  `CREATE_BY` varchar(200)  NULL COMMENT '创建人ID',\n" +
                "  `CREATE_DATE` TIMESTAMP(6)  NULL COMMENT '创建时间',\n" +
                "  `CREATE_NAME` varchar(200)  NULL COMMENT '创建人名称',\n" +
                "  `UPDATE_BY` varchar(200)  NULL COMMENT '修改人',\n" +
                "  `UPDATE_DATE` TIMESTAMP(6)  NULL COMMENT '修改时间',\n" +
                "  `UPDATE_NAME` varchar(200)  NULL COMMENT '修改人姓名',\n" +
                "  `BIRTH` date  NULL COMMENT '出生日期',\n" +
                "  `COUNTY` varchar(1275)  NULL COMMENT '国籍',\n" +
                "  `DEP_NAME` varchar(1275)  NULL COMMENT '旅客所在公司部门',\n" +
                "  `EMAIL` varchar(640)  NULL COMMENT 'EMAIL',\n" +
                "  `FARE_CATEGORY` BIGINT  NULL COMMENT '旅客类别',\n" +
                "  `FARE_NAME` varchar(1275)  NOT NULL COMMENT '订单旅客姓名',\n" +
                "  `FARE_TYPE` BIGINT  NULL COMMENT '旅客类型',\n" +
                "  `FARE_TYPECN` varchar(1275)  NULL COMMENT '旅客类型名称',\n" +
                "  `FFP_CARD_NO` varchar(1275)  NULL COMMENT '常客卡号',\n" +
                "  `FOOD_CODE` varchar(1275)  NULL COMMENT '餐食代码',\n" +
                "  `FOOD_TYPE` BIGINT  NULL COMMENT '餐食代码',\n" +
                "  `GENDER` BIGINT  NULL COMMENT '性别',\n" +
                "  `ID_CODE` varchar(160)  NULL COMMENT '旅客证件号',\n" +
                "  `ID_CODEGP` varchar(150)  NULL COMMENT '旅客证件号码GP',\n" +
                "  `ID_EXPIRY_DATE` TIMESTAMP(6)  NULL COMMENT '证件有效期',\n" +
                "  `ID_TYPE` BIGINT  NULL COMMENT '旅客证件类型',\n" +
                "  `ID_TYPECN` varchar(1275)  NULL COMMENT '证件名称',\n" +
                "  `ID_TYPEGP` BIGINT  NULL COMMENT '旅客证件类型GP',\n" +
                "  `IS_SEND_SMS` tinyINT  NULL COMMENT '是否发送短信',\n" +
                "  `ISSUE_COUNTRY` varchar(1275)  NULL COMMENT '证件签发国',\n" +
                "  `MOBILE` varchar(1275)  NULL COMMENT '旅客电话',\n" +
                "  `PARENT_NAME` varchar(1275)  NULL COMMENT '父亲名字',\n" +
                "  `PRICE_ALL_FEE` decimal(19,2)  NULL COMMENT '总税',\n" +
                "  `PRICE_BUILD_FEE` decimal(19,2)  NULL COMMENT '机场建设费',\n" +
                "  `PRICE_CABIN_PRICE` decimal(19,2)  NULL COMMENT '舱位原价',\n" +
                "  `PRICE_CURRENCY` INT  NULL COMMENT '支付币种',\n" +
                "  `PRICE_EXCHANGE_RATE` decimal(10,4)  NULL COMMENT '汇率',\n" +
                "  `PRICE_FUEL_FEE` decimal(19,2)  NULL COMMENT '机场燃油费',\n" +
                "  `PRICE_OB_FEE` decimal(19,2)  NULL COMMENT '换开变更费',\n" +
                "  `PRICE_TICKET_PRICE` decimal(19,2)  NULL COMMENT '机票价',\n" +
                "  `PRICE_TOTAL_PRICE` decimal(19,2)  NULL COMMENT '总价',\n" +
                "  `SPECIAL_TYPE` INT  NULL COMMENT '特殊服务项',\n" +
                "  `TAXES_JSON` varchar(5000)  NULL COMMENT '税明细json',\n" +
                "  `US_ADDRESS` varchar(500)  NULL COMMENT '美国航线地址',\n" +
                "  `US_CITY` varchar(200)  NULL COMMENT '美国航线城市名',\n" +
                "  `ZHIWU` varchar(640)  NULL COMMENT '特殊服务VIP  职务',\n" +
                "  `ZIP_CODE` varchar(100)  NULL COMMENT '美国航线邮编',\n" +
                "  `BUS_ORDER_PKID` varchar(1275)  NULL COMMENT '订单ID',\n" +
                "  `INSURE_ORDER_PKID` varchar(1275)  NULL COMMENT '订单ID',\n" +
                "  `ORDER_PKID` varchar(1275)  NULL COMMENT '订单ID',\n" +
                "  `INDEX_IN_PNR` varchar(15)  NULL COMMENT '在pnr中的序号',\n" +
                "  `FARE_NAME_CN` varchar(1275)  NULL COMMENT '订单旅客姓名中文国际GP用',\n" +
                "  `GOV_BIN` varchar(1275)  NULL COMMENT '政采卡bin',\n" +
                "  `GOV_BUGDET_NAME` varchar(1275)  NULL COMMENT '政采预算单位名称',\n" +
                "  `FARE_NAMEEN` varchar(1275)  NULL COMMENT '用于婴儿的SSR INFT中的英文名',\n" +
                "  `GMJC_RMK` varchar(320)  NULL COMMENT '革残警残军官备注',\n" +
                "  `LYX_ORDERS` varchar(1275)  NULL COMMENT '鲁彦行订单',\n" +
                "  `ETL_CREATE_TIME` TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
                "  `ETL_UPDATE_TIME` TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
                "  `ETL_DATE` date  NULL COMMENT '数据ETL日期'" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_KFBP_BUS_FARE',\n" +
                "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //") WITH (\n" +
        //"    'connector' = 'jdbc',\n" +
        //"    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
        //"    'table-name' = 'T_ODS_KFBP_BUS_FARE', -- 替换为实际的表名\n" +
        //"    'username' = '"+Constants.ODS_USER+"',\n" +
        //"    'password' = '"+Constants.ODS_PWD+"'\n" +
        //",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
        //")");
        log.info(" ExtractTOdsKfbpBusFare executeSql create table for doris end");
        String extractSql = "INSERT INTO T_ODS_KFBP_BUS_FARE(\n" +
                "    PKID,\n" +
                " CREATE_BY,\n" +
                " CREATE_DATE,\n" +
                " CREATE_NAME,\n" +
                " UPDATE_BY,\n" +
                " UPDATE_DATE,\n" +
                " UPDATE_NAME,\n" +
                " BIRTH,\n" +
                " COUNTY,\n" +
                " DEP_NAME,\n" +
                " EMAIL,\n" +
                " FARE_CATEGORY,\n" +
                " FARE_NAME,\n" +
                " FARE_TYPE,\n" +
                " FARE_TYPECN,\n" +
                " FFP_CARD_NO,\n" +
                " FOOD_CODE,\n" +
                " FOOD_TYPE,\n" +
                " GENDER,\n" +
                " ID_CODE,\n" +
                " ID_CODEGP,\n" +
                " ID_EXPIRY_DATE,\n" +
                " ID_TYPE,\n" +
                " ID_TYPECN,\n" +
                " ID_TYPEGP,\n" +
                " IS_SEND_SMS,\n" +
                " ISSUE_COUNTRY,\n" +
                " MOBILE,\n" +
                " PARENT_NAME,\n" +
                " PRICE_ALL_FEE,\n" +
                " PRICE_BUILD_FEE,\n" +
                " PRICE_CABIN_PRICE,\n" +
                " PRICE_CURRENCY,\n" +
                " PRICE_EXCHANGE_RATE,\n" +
                " PRICE_FUEL_FEE,\n" +
                " PRICE_OB_FEE,\n" +
                " PRICE_TICKET_PRICE,\n" +
                " PRICE_TOTAL_PRICE,\n" +
                " SPECIAL_TYPE,\n" +
                " TAXES_JSON,\n" +
                " US_ADDRESS,\n" +
                " US_CITY,\n" +
                " ZHIWU,\n" +
                " ZIP_CODE,\n" +
                " BUS_ORDER_PKID,\n" +
                " INSURE_ORDER_PKID,\n" +
                " ORDER_PKID,\n" +
                " INDEX_IN_PNR,\n" +
                " FARE_NAME_CN,\n" +
                " GOV_BIN,\n" +
                " GOV_BUGDET_NAME,\n" +
                " FARE_NAMEEN,\n" +
                " GMJC_RMK,\n" +
                " LYX_ORDERS,\n" +
                " ETL_CREATE_TIME,\n" +
                " ETL_UPDATE_TIME,\n" +
                " ETL_DATE)" +

                "SELECT\n" +
                " PKID,\n" +
                " CREATE_BY,\n" +
                " CREATE_DATE,\n" +
                " CREATE_NAME,\n" +
                " UPDATE_BY,\n" +
                " UPDATE_DATE,\n" +
                " UPDATE_NAME,\n" +
                " BIRTH,\n" +
                " COUNTY,\n" +
                " DEP_NAME,\n" +
                " EMAIL,\n" +
                " FARE_CATEGORY,\n" +
                " FARE_NAME,\n" +
                " FARE_TYPE,\n" +
                " FARE_TYPECN,\n" +
                " sm4_encrypt(FFP_CARD_NO, '" + Constants.SM4_KEY + "') FFP_CARD_NO,\n" +
                " FOOD_CODE,\n" +
                " FOOD_TYPE,\n" +
                " GENDER,\n" +
                " sm4_encrypt(ID_CODE, '" + Constants.SM4_KEY + "') ID_CODE,\n" +
                " ID_CODEGP,\n" +
                " ID_EXPIRY_DATE,\n" +
                " ID_TYPE,\n" +
                " ID_TYPECN,\n" +
                " ID_TYPEGP,\n" +
                " IS_SEND_SMS,\n" +
                " ISSUE_COUNTRY,\n" +
                " sm4_encrypt(MOBILE, '" + Constants.SM4_KEY + "') MOBILE,\n" +
                " PARENT_NAME,\n" +
                " PRICE_ALL_FEE,\n" +
                " PRICE_BUILD_FEE,\n" +
                " PRICE_CABIN_PRICE,\n" +
                " PRICE_CURRENCY,\n" +
                " PRICE_EXCHANGE_RATE,\n" +
                " PRICE_FUEL_FEE,\n" +
                " PRICE_OB_FEE,\n" +
                " PRICE_TICKET_PRICE,\n" +
                " PRICE_TOTAL_PRICE,\n" +
                " SPECIAL_TYPE,\n" +
                " TAXES_JSON,\n" +
                " US_ADDRESS,\n" +
                " US_CITY,\n" +
                " ZHIWU,\n" +
                " ZIP_CODE,\n" +
                " BUS_ORDER_PKID,\n" +
                " INSURE_ORDER_PKID,\n" +
                " ORDER_PKID,\n" +
                " INDEX_IN_PNR,\n" +
                " FARE_NAME_CN,\n" +
                " GOV_BIN,\n" +
                " GOV_BUGDET_NAME,\n" +
                " FARE_NAMEEN,\n" +
                " GMJC_RMK,\n" +
                " LYX_ORDERS,\n" +
                " CURRENT_TIMESTAMP AS ETL_CREATE_TIME,\n" +
                " CURRENT_TIMESTAMP AS ETL_UPDATE_TIME,\n" +
                " CAST('" + etlDate + "' AS DATE) ETL_DATE\n" +
                " FROM BUS_FARE "
                + " WHERE  CREATE_DATE BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                +" OR  UPDATE_DATE BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                ;
        log.info(" ExtractTOdsKfbpBusFare executeSql extract start");
        TableResult result = tEnv.executeSql(extractSql);
        log.info(" ExtractTOdsKfbpBusFare executeSql extract end");

        result.print();

        log.info(" ExtractTOdsKfbpBusFare data transfer end");


    }

}
