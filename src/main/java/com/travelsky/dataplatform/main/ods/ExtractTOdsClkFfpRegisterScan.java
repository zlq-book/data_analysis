package com.travelsky.dataplatform.main.ods;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.OracleAESDecryptor;
import com.travelsky.dataplatform.udf.OracleDESDecryptor;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.UUID;

public class ExtractTOdsClkFfpRegisterScan {
    private static Session sshSession;

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (args.length < 1) {
            System.exit(0);
        }
        String etlDate = args[0];
        String startDate = etlDate;
        String endDate = etlDate;
        if (args.length > 2) {
            startDate = args[1];
            endDate = args[2];
        }
        String sm4key = Constants.SM4_KEY;
        System.out.println("ExtractTOdsClkFfpRegisterScan etl_date:" + etlDate + ", startDate:" + startDate + ", endDate:" + endDate);
        
        boolean useSshTunnel = isLocalEnvironment();
        String jdbcUrl;

        if (useSshTunnel) {
            String sshProxyHost = "10.31.0.20";
            int sshProxyPort = 50082;
            String sshUsername = System.getProperty("ssh.username", "");
            String sshPassword = System.getProperty("ssh.password", "");
            
            String oracleHost = Constants.CLK_IP;
            int oraclePort = Integer.parseInt(Constants.CLK_PORT);
            
            try {
                int localPort = establishSshTunnel(sshProxyHost, sshProxyPort, sshUsername, sshPassword, oracleHost, oraclePort);
                jdbcUrl = "jdbc:oracle:thin:@//localhost:" + localPort + "/" + Constants.CLK_DB;
//                jdbcUrl = "jdbc:oracle:thin:@//" + Constants.CLK_IP + ":" + Constants.CLK_PORT + "/" + Constants.CLK_DB;
                System.out.println("使用SSH隧道连接Oracle，本地端口: " + localPort);
            } catch (Exception e) {
                throw new IOException("建立SSH隧道失败: " + e.getMessage(), e);
            }
            
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                closeSshTunnel();
            }));
        } else {
            jdbcUrl = "jdbc:oracle:thin:@//" + Constants.CLK_IP + ":" + Constants.CLK_PORT + "/" + Constants.CLK_DB;
            System.out.println("直接连接Oracle数据库: " + Constants.CLK_IP + ":" + Constants.CLK_PORT);
        }


        int parallelism = 1;
        env.setParallelism(parallelism);
        System.out.println("设置Flink并行度为: " + parallelism);
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsClkFfpRegisterScan");
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        tEnv.createTemporarySystemFunction("des_decrypt", OracleDESDecryptor.class);
        
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        String labelPrefix = timestamp + uuid.toString().replace("-", "");
        
        StringBuffer ffpRegisterScanSql = new StringBuffer();
        ffpRegisterScanSql.append("CREATE TABLE FFP_REGISTER_SCAN (\n");
        ffpRegisterScanSql.append("ID VARCHAR(64) NOT NULL,\n");
        ffpRegisterScanSql.append("TRADEID VARCHAR(30),\n");
        ffpRegisterScanSql.append("CREATE_TIME DATE,\n");
        ffpRegisterScanSql.append("CNNAME VARCHAR(50),\n");
        ffpRegisterScanSql.append("FLTDATE VARCHAR(30),\n");
        ffpRegisterScanSql.append("FLTNUM VARCHAR(30),\n");
        ffpRegisterScanSql.append("RESERVED_SEAT VARCHAR(30),\n");
        ffpRegisterScanSql.append("FPHONE VARCHAR(100),\n");
        ffpRegisterScanSql.append("FCARD_TYPE VARCHAR(30),\n");
        ffpRegisterScanSql.append("MATCH_FLAG CHAR(1),\n");
        ffpRegisterScanSql.append("FFP_REGISTER_INFO_ID VARCHAR(64),\n");
        ffpRegisterScanSql.append("REMARKS VARCHAR(255)\n");
        ffpRegisterScanSql.append(") WITH (\n");
        ffpRegisterScanSql.append("    'connector' = 'jdbc',\n");
        ffpRegisterScanSql.append("    'url' = '").append(jdbcUrl).append("',\n");
        ffpRegisterScanSql.append("    'table-name' = 'SCFFP.FFP_REGISTER_SCAN',\n");
        ffpRegisterScanSql.append("    'username' = '").append(Constants.CLK_USER).append("',\n");
        ffpRegisterScanSql.append("    'password' = '").append(Constants.CLK_PWD).append("',\n");
        ffpRegisterScanSql.append("    'driver' = '").append(Constants.ORACLE_DRIVER).append("',\n");
        ffpRegisterScanSql.append("    'scan.fetch-size' = '10000',\n");
        ffpRegisterScanSql.append("    'scan.auto-commit' = 'false'\n");
        ffpRegisterScanSql.append(")");
        tEnv.executeSql(ffpRegisterScanSql.toString());

        System.out.println("数据范围: " + startDate + " 至 " + endDate);
        System.out.println("使用 DataStream 方式处理数据流");
        
        String selectSql = "SELECT " +
                "ID,\n" +
                "TRADEID,\n" +
                "CREATE_TIME,\n" +
                "CNNAME,\n" +
                "FLTDATE,\n" +
                "FLTNUM,\n" +
                "RESERVED_SEAT,\n" +
                "sm4_encrypt(\n" +
                "    CASE \n" +
                "        WHEN CREATE_TIME < DATE '2022-01-01' THEN des_decrypt(FPHONE, '" + Constants.LYGJ_ZB_AES_KEY + "')\n" +
                "        ELSE aes_decrypt(FPHONE, '" + Constants.LYGJ_AES_KEY + "')\n" +
                "    END, '" + sm4key + "') FPHONE,\n" +
                "FCARD_TYPE,\n" +
                "MATCH_FLAG,\n" +
                "FFP_REGISTER_INFO_ID,\n" +
                "REMARKS\n" +
                "FROM FFP_REGISTER_SCAN " +
                "WHERE CREATE_TIME >= DATE '" + startDate + "' AND CREATE_TIME < DATE '" + endDate + "' + INTERVAL '1' DAY";
        
        Table sourceTable = tEnv.sqlQuery(selectSql);
        DataStream<Row> rowStream = tEnv.toDataStream(sourceTable);
        
        TypeInformation<FfpRegisterScanEntity> typeInfo = Types.POJO(FfpRegisterScanEntity.class);
        DataStream<FfpRegisterScanEntity> entityStream = rowStream
                .map(row -> rowToEntity(row, etlDate))
                .returns(typeInfo)
                .name("Row-To-Entity");
        
        DorisSink<FfpRegisterScanEntity> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.ODS_DB,
                "T_ODS_CLK_FFP_REGISTER_SCAN",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.ODS_USER,
                Constants.ODS_PWD
        );
        
        entityStream.sinkTo(dorisSink).name("Doris-Sink");
        
        System.out.println("开始执行数据抽取...");
        long startTime = System.currentTimeMillis();
        try {
            env.execute("ExtractTOdsClkFfpRegisterScan");
            long duration = System.currentTimeMillis() - startTime;
            System.out.println("数据抽取完成，耗时: " + (duration / 1000) + " 秒");
        } catch (Exception e) {
            System.err.println("数据抽取过程中出现异常: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
        
        if (useSshTunnel) {
            closeSshTunnel();
        }
    }
    
    private static boolean isLocalEnvironment() {
        String sshTunnel = System.getProperty("ssh.tunnel.enable");
        return "true".equalsIgnoreCase(sshTunnel);
    }
    
    private static int establishSshTunnel(String sshHost, int sshPort, String username, String password, 
                                          String remoteHost, int remotePort) throws JSchException {
        JSch jsch = new JSch();
        Session session = jsch.getSession(username, sshHost, sshPort);
        session.setPassword(password);
        
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        config.put("UserKnownHostsFile", "/dev/null");
        session.setConfig(config);
        
        session.connect(30000);
        
        int localPort = session.setPortForwardingL(0, remoteHost, remotePort);
        sshSession = session;
        
        return localPort;
    }
    
    private static void closeSshTunnel() {
        if (sshSession != null && sshSession.isConnected()) {
            sshSession.disconnect();
            sshSession = null;
        }
    }
    
    private static FfpRegisterScanEntity rowToEntity(Row row, String etlDate) {
        FfpRegisterScanEntity entity = new FfpRegisterScanEntity();
        try {
            entity.setId(row.getField(0) != null ? row.getField(0).toString() : null);
            entity.setTradeid(row.getField(1) != null ? row.getField(1).toString() : null);
            entity.setCreateTime((Date) row.getField(2));
            entity.setCnname(row.getField(3) != null ? row.getField(3).toString() : null);
            entity.setFltdate(row.getField(4) != null ? row.getField(4).toString() : null);
            entity.setFltnum(row.getField(5) != null ? row.getField(5).toString() : null);
            entity.setReservedSeat(row.getField(6) != null ? row.getField(6).toString() : null);
            entity.setFphone(row.getField(7) != null ? row.getField(7).toString() : null);
            entity.setFcardType(row.getField(8) != null ? row.getField(8).toString() : null);
            entity.setMatchFlag(row.getField(9) != null ? row.getField(9).toString() : null);
            entity.setFfpRegisterInfoId(row.getField(10) != null ? row.getField(10).toString() : null);
            entity.setRemarks(row.getField(11) != null ? row.getField(11).toString() : null);
            
            Timestamp currentTime = new Timestamp(System.currentTimeMillis());
            entity.setEtlCreateTime(currentTime);
            entity.setEtlUpdateTime(currentTime);
            entity.setEtlDate(Date.valueOf(etlDate));
        } catch (Exception e) {
            System.err.println("Row转实体失败: " + e.getMessage());
            throw new RuntimeException("Row转实体失败", e);
        }
        return entity;
    }
    
    private static int calculateOptimalParallelism(String startDate, String endDate) {
        try {
            java.time.LocalDate start = java.time.LocalDate.parse(startDate);
            java.time.LocalDate end = java.time.LocalDate.parse(endDate);
            long days = java.time.temporal.ChronoUnit.DAYS.between(start, end) + 1;
            
            int parallelism;
            if (days <= 1) {
                parallelism = 8;
            } else if (days <= 7) {
                parallelism = 12;
            } else if (days <= 30) {
                parallelism = 16;
            } else {
                parallelism = 20;
            }
            
            int maxParallelism = Math.min(parallelism, Runtime.getRuntime().availableProcessors() * 2);
            return Math.max(4, maxParallelism);
        } catch (Exception e) {
            return 8;
        }
    }
    
}

