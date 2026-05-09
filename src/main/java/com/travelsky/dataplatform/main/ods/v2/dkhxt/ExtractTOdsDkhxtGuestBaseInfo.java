package com.travelsky.dataplatform.main.ods.v2.dkhxt;

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
public class ExtractTOdsDkhxtGuestBaseInfo {
    public static Logger log = LoggerFactory.getLogger(ExtractTOdsDkhxtGuestBaseInfo.class);
    public static void main(String[] args) throws IOException {
        log.info("ExtractTOdsDkhxtGuestBaseInfo data transfer start");
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
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsDkhxtGuestBaseInfo");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建上游ORACLE数据源表
        log.info("ExtractTOdsDkhxtGuestBaseInfo executeSql create table for source start");
        tEnv.executeSql("CREATE TABLE GUEST_BASEINFO (\n" +
                " GUEST_NGUESTID BIGINT NOT NULL,\n" +
                " GUEST_SCODE  VARCHAR(20) NOT NULL,\n" +
                " GUEST_SORGCODE  VARCHAR(100) NOT NULL,\n" +
                " GUEST_SORGNAME  VARCHAR(200) NOT NULL,\n" +
                " GUEST_SMANAGERCODE  VARCHAR(100) NOT NULL,\n" +
                " GUEST_SMANAGERNAME  VARCHAR(50) NOT NULL,\n" +
                " GUEST_SNAME  VARCHAR(100),\n" +
                " GUEST_SPROPTY  VARCHAR(20),\n" +
                " GUEST_SINDUSTRY  VARCHAR(80),\n" +
                " GUEST_SSTATUS  VARCHAR(20),\n" +
                " GUEST_SADDR  VARCHAR(100),\n" +
                " GUEST_SEMAIL  VARCHAR(100),\n" +
                " GUEST_STEL  VARCHAR(20),\n" +
                " GUEST_SFAX  VARCHAR(20),\n" +
                " GUEST_SCONTACT  VARCHAR(50),\n" +
                " GUEST_SBUYHABITS  VARCHAR(50),\n" +
                " GUEST_SROUTE  VARCHAR(50),\n" +
                " GUEST_SISIMPORT  VARCHAR(10),\n" +
                " GUEST_SCONTACTJOB  VARCHAR(50),\n" +
                " GUEST_SCONTACTPHONE  VARCHAR(50),\n" +
                " GUEST_SISB2C  VARCHAR(10),\n" +
                " GUEST_SAUTOUPATE  VARCHAR(10),\n" +
                " GUEST_SVALIDCODE  VARCHAR(20),\n" +
                " GUEST_SISCOOPER  VARCHAR(10),\n" +
                " GUEST_SCREATER  VARCHAR(20),\n" +
                " GUEST_SCREATETIME TIMESTAMP(6),\n" +
                " GUEST_SUPDATER  VARCHAR(20),\n" +
                " GUEST_SUPDATETIME TIMESTAMP(6),\n" +
                " GUEST_SDELETER  VARCHAR(20),\n" +
                " GUEST_SDELETETIME TIMESTAMP(6),\n" +
                " GUEST_SCODE1  VARCHAR(20),\n" +
                " GUEST_SCODE2  VARCHAR(20),\n" +
                " GUEST_ISDELETE  VARCHAR(10) ,\n" +
                " GUEST_ISTCOOPER  VARCHAR(10) ,\n" +
                " GUEST_POSTCODE  VARCHAR(20),\n" +
                " SUREFLAG  VARCHAR(10),\n" +
                " GUEST_PASSWORD  VARCHAR(30),\n" +
                " GRADE_ATTENTION  VARCHAR(10),\n" +
                " DEPART_ATTENTION  VARCHAR(10),\n" +
                " GUEST_GRADE  VARCHAR(10),\n" +
                " GUEST_GRADESD  VARCHAR(10)" +

                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.DKHXT_IP + ":" + Constants.DKHXT_PORT + "/" + Constants.DKHXT_DB + "',\n" +
                "    'table-name' = '" + Constants.DKHXT_SCHEMA + ".GUEST_BASEINFO', \n" +
                "    'username' = '" + Constants.DKHXT_USER + "',\n" +
                "    'password' = '" + Constants.DKHXT_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        log.info("ExtractTOdsDkhxtGuestBaseInfo executeSql create table for source end");
        //创建Doris目标表
        log.info("ExtractTOdsDkhxtGuestBaseInfo executeSql create table for doris start");
        tEnv.executeSql("CREATE TABLE `T_ODS_DKHXT_GUEST_BASEINFO` (\n" +
                "  `GUEST_NGUESTID` bigint NOT NULL COMMENT  '大客户ID ',\n" +
                "  `GUEST_SCODE` varchar(100)   NOT NULL COMMENT  '大客户编号 ',\n" +
                "  `GUEST_SORGCODE` varchar(500)   NOT NULL COMMENT  '营业部编号 ',\n" +
                "  `GUEST_SORGNAME` varchar(1000)   NOT NULL COMMENT  '营业部名称 ',\n" +
                "  `GUEST_SMANAGERCODE` varchar(500)   NOT NULL COMMENT  '客户经理编号 ',\n" +
                "  `GUEST_SMANAGERNAME` varchar(250)   NOT NULL COMMENT  '客户经理名称 ',\n" +
                "  `GUEST_SNAME` varchar(500)   NULL COMMENT  '单位名称 ',\n" +
                "  `GUEST_SPROPTY` varchar(100)   NULL COMMENT  '客户性质（企业0/机关1/事业2/其它3） ',\n" +
                "  `GUEST_SINDUSTRY` varchar(400)   NULL COMMENT  '所属行业 ',\n" +
                "  `GUEST_SSTATUS` varchar(100)   NULL COMMENT  '客户状态(未签0、新签1、续签2、终止3) ',\n" +
                "  `GUEST_SADDR` varchar(500)   NULL COMMENT  '单位地址 ',\n" +
                "  `GUEST_SEMAIL` varchar(500)   NULL COMMENT  'Email ',\n" +
                "  `GUEST_STEL` varchar(100)   NULL COMMENT  '单位电话 ',\n" +
                "  `GUEST_SFAX` varchar(100)   NULL COMMENT  '传真 ',\n" +
                "  `GUEST_SCONTACT` varchar(250)   NULL COMMENT  '单位联系人 ',\n" +
                "  `GUEST_SBUYHABITS` varchar(250)   NULL COMMENT  '购买习惯001网络，002电话 ',\n" +
                "  `GUEST_SROUTE` varchar(250)   NULL COMMENT  '主要航线 ',\n" +
                "  `GUEST_SISIMPORT` varchar(50)   NULL COMMENT  '是否重点客户(Y是N不是) ',\n" +
                "  `GUEST_SCONTACTJOB` varchar(250)   NULL COMMENT  '单位联系人职务 ',\n" +
                "  `GUEST_SCONTACTPHONE` varchar(250)   NULL COMMENT  '单位联系人电话 ',\n" +
                "  `GUEST_SISB2C` varchar(50)   NULL COMMENT  '是否B2C中小型(Y是N不是) ',\n" +
                "  `GUEST_SAUTOUPATE` varchar(50)   NULL COMMENT  '是否自动更新(Y是N不是) ',\n" +
                "  `GUEST_SVALIDCODE` varchar(100)   NULL COMMENT  '当前有效编号 ',\n" +
                "  `GUEST_SISCOOPER` varchar(50)   NULL COMMENT  '是否国山合作(Y是N不是) ',\n" +
                "  `GUEST_SCREATER` varchar(100)   NULL COMMENT  '创建人 ',\n" +
                "  `GUEST_SCREATETIME`  TIMESTAMP(6)   NULL COMMENT  '创建时间 ',\n" +
                "  `GUEST_SUPDATER` varchar(100)   NULL COMMENT  '修改人 ',\n" +
                "  `GUEST_SUPDATETIME`  TIMESTAMP(6)   NULL COMMENT  '修改时间 ',\n" +
                "  `GUEST_SDELETER` varchar(100)   NULL COMMENT  '删除人 ',\n" +
                "  `GUEST_SDELETETIME`  TIMESTAMP(6)   NULL COMMENT  '删除时间 ',\n" +
                "  `GUEST_SCODE1` varchar(100)   NULL COMMENT  '国山合作编号1 ',\n" +
                "  `GUEST_SCODE2` varchar(100)   NULL COMMENT  '国山合作编号2 ',\n" +
                "  `GUEST_ISDELETE` varchar(50)   NULL COMMENT  '是否删除Y已删除N未删除 ',\n" +
                "  `GUEST_ISTCOOPER` varchar(50)   NULL COMMENT  '是否商旅合作(Y是N不是) ',\n" +
                "  `GUEST_POSTCODE` varchar(100)   NULL COMMENT  '邮编 ',\n" +
                "  `SUREFLAG` varchar(50)   NULL COMMENT  'N未确认/Y已确认/S已办结 ',\n" +
                "  `GUEST_PASSWORD` varchar(150)   NULL COMMENT  '大客户前端密码 ',\n" +
                "  `GRADE_ATTENTION` varchar(50)   NULL,\n" +
                "  `DEPART_ATTENTION` varchar(50)   NULL,\n" +
                "  `GUEST_GRADE` varchar(50)   NULL,\n" +
                "  `GUEST_GRADESD` varchar(50)   NULL,\n" +
                "  `ETL_CREATE_TIME`  TIMESTAMP(6)  NULL  COMMENT  '数据入仓时间 ',\n" +
                "  `ETL_UPDATE_TIME`  TIMESTAMP(6)   NULL  COMMENT  '数据在数仓更新时间时间 ',\n" +
                "  `ETL_DATE` date   NULL COMMENT  '数据ETL日期 '" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_DKHXT_GUEST_BASEINFO',\n" +
                "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
                //") WITH (\n" +
                //"    'connector' = 'jdbc',\n" +
                //"    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
                //"    'table-name' = 'T_ODS_DKHXT_GUEST_BASEINFO', -- 替换为实际的表名\n" +
                //"    'username' = '"+Constants.ODS_USER+"',\n" +
                //"    'password' = '"+Constants.ODS_PWD+"'\n" +
                //",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
                //")");
        log.info("ExtractTOdsDkhxtGuestBaseInfo executeSql create table for doris end");
        String extractSql = "INSERT INTO T_ODS_DKHXT_GUEST_BASEINFO (\n" +
                " GUEST_NGUESTID,\n" +
                " GUEST_SCODE,\n" +
                " GUEST_SORGCODE,\n" +
                " GUEST_SORGNAME,\n" +
                " GUEST_SMANAGERCODE,\n" +
                " GUEST_SMANAGERNAME,\n" +
                " GUEST_SNAME,\n" +
                " GUEST_SPROPTY,\n" +
                " GUEST_SINDUSTRY,\n" +
                " GUEST_SSTATUS,\n" +
                " GUEST_SADDR,\n" +
                " GUEST_SEMAIL,\n" +
                " GUEST_STEL,\n" +
                " GUEST_SFAX,\n" +
                " GUEST_SCONTACT,\n" +
                " GUEST_SBUYHABITS,\n" +
                " GUEST_SROUTE,\n" +
                " GUEST_SISIMPORT,\n" +
                " GUEST_SCONTACTJOB,\n" +
                " GUEST_SCONTACTPHONE,\n" +
                " GUEST_SISB2C,\n" +
                " GUEST_SAUTOUPATE,\n" +
                " GUEST_SVALIDCODE,\n" +
                " GUEST_SISCOOPER,\n" +
                " GUEST_SCREATER,\n" +
                " GUEST_SCREATETIME,\n" +
                " GUEST_SUPDATER,\n" +
                " GUEST_SUPDATETIME,\n" +
                " GUEST_SDELETER,\n" +
                " GUEST_SDELETETIME,\n" +
                " GUEST_SCODE1,\n" +
                " GUEST_SCODE2,\n" +
                " GUEST_ISDELETE,\n" +
                " GUEST_ISTCOOPER,\n" +
                " GUEST_POSTCODE,\n" +
                " SUREFLAG,\n" +
                " GUEST_PASSWORD,\n" +
                " GRADE_ATTENTION,\n" +
                " DEPART_ATTENTION,\n" +
                " GUEST_GRADE,\n" +
                " GUEST_GRADESD,\n" +
                " ETL_CREATE_TIME,\n" +
                " ETL_UPDATE_TIME,\n" +
                " ETL_DATE) " +

                "SELECT\n" +
                " GUEST_NGUESTID,\n" +
                " GUEST_SCODE,\n" +
                " GUEST_SORGCODE,\n" +
                " GUEST_SORGNAME,\n" +
                " GUEST_SMANAGERCODE,\n" +
                " GUEST_SMANAGERNAME,\n" +
                " GUEST_SNAME,\n" +
                " GUEST_SPROPTY,\n" +
                " GUEST_SINDUSTRY,\n" +
                " GUEST_SSTATUS,\n" +
                " GUEST_SADDR,\n" +
                " GUEST_SEMAIL,\n" +
                " GUEST_STEL,\n" +
                " GUEST_SFAX,\n" +
                " GUEST_SCONTACT,\n" +
                " GUEST_SBUYHABITS,\n" +
                " GUEST_SROUTE,\n" +
                " GUEST_SISIMPORT,\n" +
                " GUEST_SCONTACTJOB,\n" +
                " sm4_encrypt(GUEST_SCONTACTPHONE, '" + Constants.SM4_KEY + "') GUEST_SCONTACTPHONE,\n" +
                " GUEST_SISB2C,\n" +
                " GUEST_SAUTOUPATE,\n" +
                " GUEST_SVALIDCODE,\n" +
                " GUEST_SISCOOPER,\n" +
                " GUEST_SCREATER,\n" +
                " GUEST_SCREATETIME,\n" +
                " GUEST_SUPDATER,\n" +
                " GUEST_SUPDATETIME,\n" +
                " GUEST_SDELETER,\n" +
                " GUEST_SDELETETIME,\n" +
                " GUEST_SCODE1,\n" +
                " GUEST_SCODE2,\n" +
                " GUEST_ISDELETE,\n" +
                " GUEST_ISTCOOPER,\n" +
                " GUEST_POSTCODE,\n" +
                " SUREFLAG,\n" +
//                " GUEST_PASSWORD,\n" +
                " sm4_encrypt(GUEST_PASSWORD, '" + Constants.SM4_KEY + "') GUEST_PASSWORD,\n" +
                " GRADE_ATTENTION,\n" +
                " DEPART_ATTENTION,\n" +
                " GUEST_GRADE,\n" +
                " GUEST_GRADESD,\n" +
                " CURRENT_TIMESTAMP AS ETL_CREATE_TIME,\n" +
                " CURRENT_TIMESTAMP AS ETL_UPDATE_TIME,\n" +
                " CAST('" + etlDate + "' AS DATE) ETL_DATE\n" +
                " FROM GUEST_BASEINFO "
                + " WHERE  GUEST_SCREATETIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                +" OR  GUEST_SUPDATETIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                +" OR  GUEST_SDELETETIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                ;
        log.info("ExtractTOdsDkhxtGuestBaseInfo executeSql extract start");
        TableResult result = tEnv.executeSql(extractSql);
        log.info("ExtractTOdsDkhxtGuestBaseInfo executeSql extract end");

        result.print();

        log.info("ExtractTOdsDkhxtGuestBaseInfo data transfer end");


    }

}
