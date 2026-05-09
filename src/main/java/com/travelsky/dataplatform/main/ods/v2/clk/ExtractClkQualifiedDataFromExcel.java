package com.travelsky.dataplatform.main.ods.v2.clk;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.clk.TOdsClkQualifiedNoRegistration;
import com.travelsky.dataplatform.module.ods.clk.TOdsClkQualifiedRegistration;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.SM4Utils;
import com.travelsky.trp.usercenter.data.analysis.utils.EasyExcelUtils;
import com.travelsky.trp.usercenter.data.analysis.utils.FTPUtils;
import com.travelsky.trp.usercenter.data.analysis.utils.FTPConfigUtils;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

/**
 * 国航系达标人群数据Excel文件转储脚本
 * 从SFTP服务器读取达标人群报表的2个sheet页数据并导入到Doris数据库
 * 数据流程:FTP(通过SOCKS5代理) → HDFS → Flink DataStream → Doris
 * <p>
 * 支持的sheet页：
 * 1. 达标人群（报名） → T_ODS_CLK_QUALIFIED_REGISTRATION
 * 2. 达标人群（无需报名） → T_ODS_CLK_QUALIFIED_NO_REGISTRATION
 * <p>
 * 使用方式：
 * java -cp xxx ExtractClkQualifiedDataFromExcel <ETL日期>
 *
 * @author Generated
 * @date 2025-10-27
 */
public class ExtractClkQualifiedDataFromExcel {

    private static final Logger logger = LoggerFactory.getLogger(ExtractClkQualifiedDataFromExcel.class);

    public static void main(String[] args) {
        try {
            logger.info("开始执行国航系达标人群数据Excel转储任务（FTP → HDFS → Doris）...");

            // 参数校验
            if (args.length < 1) {
                logger.error("缺少ETL日期参数，格式：yyyy-MM-dd");
                logger.error("使用方式: java -cp xxx ExtractClkQualifiedDataFromExcel <ETL日期>");
                System.exit(1);
            }

            String etlDate = args[0];
            String fileDate = convertToFileDateFormat(etlDate);
            logger.info("ETL日期: {}", etlDate);
            logger.info("文件日期: {}", fileDate);

            // 步骤1：从SFTP下载文件并上传到HDFS
            logger.info("步骤1: 从FTP下载文件到HDFS...");

            // FTP配置信息(从Constants读取)
            FTPConfigUtils.FtpConfig ftpConfig = new FTPConfigUtils.FtpConfig(
                    Constants.CLK_BUSINESS_FTP_HOST,
                    Constants.CLK_BUSINESS_FTP_USERNAME,
                    Constants.CLK_BUSINESS_FTP_PWD,
                    "/changlvk"
            );

            String hdfsFilePath = downloadFromFtpToHdfs(ftpConfig, "FFP_DBRQ", fileDate);

            if (hdfsFilePath == null) {
                logger.error("从FTP下载文件到HDFS失败，FTP文件未找到，跳过本次处理");
                return;
            }

            logger.info("文件已上传到HDFS: {}", hdfsFilePath);

            // 步骤2：从HDFS读取Excel文件所有sheet页数据
            logger.info("步骤2: 从HDFS读取Excel文件数据...");
            Map<String, List<?>> excelData = readQualifiedDataFromHdfs(hdfsFilePath);

            if (excelData == null || excelData.isEmpty()) {
                logger.error("读取HDFS上的Excel文件失败或数据为空");
                System.exit(1);
            }

            // 步骤3：创建Flink执行环境
            logger.info("步骤3: 初始化Flink执行环境...");
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            CheckpointUtils.setCheckpoint(env, "ExtractClkQualifiedData-" + fileDate);

            // 步骤4：处理每个sheet页的数据并写入Doris
            logger.info("步骤4: 开始处理各sheet页数据...");

            // 4.1 处理"达标人群（报名）"
            processQualifiedRegistration(env, excelData, fileDate);

            // 4.2 处理"达标人群（无需报名）"
            processQualifiedNoRegistration(env, excelData, fileDate);

            // 步骤5：执行Flink作业
            logger.info("步骤5: 执行Flink作业...");
            env.execute("CLK-QualifiedData-SFTP-Transfer-" + fileDate);

            logger.info("国航系达标人群数据Excel转储任务执行完成！");

        } catch (Exception e) {
            logger.error("国航系达标人群数据Excel转储异常: {}", e.getMessage(), e);
        }
    }

    /**
     * 从FTP下载文件并上传到HDFS(通过SOCKS5代理)
     */
    private static String downloadFromFtpToHdfs(FTPConfigUtils.FtpConfig ftpConfig,
                                                String filePrefix,
                                                String dateSignature) {
        FTPUtils ftpUtils = null;
        FileSystem hdfs = null;
        InputStream ftpInputStream = null;
        FSDataOutputStream hdfsOutputStream = null;

        try {
            Configuration hdfsConf = buildHdfsConfiguration();
            hdfs = FileSystem.get(hdfsConf);
            logger.info("HDFS连接成功");

            ftpUtils = new FTPUtils(ftpConfig.getHost(), ftpConfig.getPort(),
                    ftpConfig.getUsername(), ftpConfig.getPassword());
            // 本地调试需要设置代理
//            ftpUtils.setProxy("127.0.0.1", 1080);

            if (!ftpUtils.connect()) {
                throw new RuntimeException("FTP连接失败");
            }

//            logger.info("FTP连接成功: {}:{}(通过SOCKS5代理 127.0.0.1:1080)",
//                    ftpConfig.getHost(), ftpConfig.getPort());

            if (ftpConfig.getRemoteDirectory() != null && !ftpConfig.getRemoteDirectory().isEmpty()) {
                if (!ftpUtils.changeWorkingDirectory(ftpConfig.getRemoteDirectory())) {
                    throw new RuntimeException("无法切换目录: " + ftpConfig.getRemoteDirectory());
                }
            }

            String fileName = findAvailableFile(ftpUtils, filePrefix, dateSignature);
            if (fileName == null) {
                logger.warn("未找到匹配文件: {}_{}", filePrefix, dateSignature);
                return null;
            }

            logger.info("找到FTP文件: {}", fileName);

            String hdfsBaseDir = Constants.CLK_FTP_HDFS != null ? Constants.CLK_FTP_HDFS : "/data/clk/ftp_transfer";
            String hdfsFilePath = hdfsBaseDir + "/" + dateSignature + "/" + fileName;
            Path hdfsPath = new Path(hdfsFilePath);

            if (!hdfs.exists(hdfsPath.getParent())) {
                hdfs.mkdirs(hdfsPath.getParent());
            }
            if (hdfs.exists(hdfsPath)) {
                hdfs.delete(hdfsPath, false);
            }

            ftpInputStream = ftpUtils.getFtpClient().retrieveFileStream(fileName);
            if (ftpInputStream == null) {
                throw new RuntimeException("无法获取FTP文件流: " + fileName);
            }

            hdfsOutputStream = hdfs.create(hdfsPath, true);
            byte[] buffer = new byte[65536];
            int bytesRead;
            long totalBytes = 0;

            while ((bytesRead = ftpInputStream.read(buffer)) != -1) {
                hdfsOutputStream.write(buffer, 0, bytesRead);
                totalBytes += bytesRead;
            }

            hdfsOutputStream.hflush();
            hdfsOutputStream.hsync();
            hdfsOutputStream.close();
            hdfsOutputStream = null;

            ftpUtils.getFtpClient().completePendingCommand();
            logger.info("文件上传到HDFS成功: {}(大小:{} MB)",
                    hdfsFilePath, totalBytes / 1024 / 1024);

            return hdfsFilePath;

        } catch (Exception e) {
            logger.error("从FTP下载并上传到HDFS失败: {}", e.getMessage(), e);
            return null;
        } finally {
            if (hdfsOutputStream != null) {
                try {
                    hdfsOutputStream.close();
                } catch (Exception e) {
                }
            }
            if (ftpInputStream != null) {
                try {
                    ftpInputStream.close();
                } catch (Exception e) {
                }
            }
            if (ftpUtils != null) {
                ftpUtils.disconnect();
            }
            if (hdfs != null) {
                try {
                    hdfs.close();
                } catch (Exception e) {
                }
            }
        }
    }

    /**
     * 智能文件查找(支持多种文件名格式)
     */
    private static String findAvailableFile(FTPUtils ftpUtils, String filePrefix, String dateSignature) {
        try {
            List<FTPFile> files = ftpUtils.listFiles(".");
            logger.info("FTP目录文件总数: {}", files.size());

            String[] patterns = {
                    filePrefix + "_" + dateSignature + ".xlsx",
                    filePrefix + "_" + dateSignature + ".xls",
                    filePrefix + dateSignature + ".xlsx",
                    filePrefix + dateSignature + ".xls",
                    filePrefix + ".xlsx",
                    filePrefix + ".xls"
            };

            for (String pattern : patterns) {
                for (FTPFile file : files) {
                    if (file.getName().equals(pattern)) {
                        logger.info("找到精确匹配文件: {}", file.getName());
                        return file.getName();
                    }
                }
            }

            for (FTPFile file : files) {
                String filename = file.getName();
                if (filename.contains(filePrefix) && filename.contains(dateSignature)
                    && (filename.endsWith(".xlsx") || filename.endsWith(".xls"))) {
                    logger.info("找到模糊匹配文件: {}", filename);
                    return filename;
                }
            }

            logger.warn("未找到匹配的文件: {}_{}*.xlsx/xls", filePrefix, dateSignature);
            return null;

        } catch (Exception e) {
            logger.error("查找文件失败: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * 构建HDFS配置（包含Kerberos认证）
     */
    private static Configuration buildHdfsConfiguration() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://cmss");
        conf.set("dfs.nameservices", "cmss");
        conf.set("dfs.ha.namenodes.cmss", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.cmss.nn1", "emr-master-001.novalocal:8020");
        conf.set("dfs.namenode.rpc-address.cmss.nn2", "emr-master-ha-001.novalocal:8020");
        conf.set("dfs.client.failover.proxy.provider.cmss",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        // 配置Kerberos认证
//        conf.set("hadoop.security.authentication", "kerberos");
//        conf.set("hadoop.security.authorization", "true");
//
//        try {
//            UserGroupInformation.setConfiguration(conf);
//            UserGroupInformation.loginUserFromKeytab(
//                    Constants.SYS_UCV_TEST_FOR_MAPREDUCE_PRINCIPAL,
//                    Constants.SYS_UCV_TEST_FOR_MAPREDUCE_KEYTAB_PATH
//            );
//            logger.info("Kerberos认证成功: {}", Constants.SYS_UCV_TEST_FOR_MAPREDUCE_PRINCIPAL);
//        } catch (Exception e) {
//            logger.error("Kerberos认证失败: {}", e.getMessage(), e);
//            throw new RuntimeException("Kerberos认证失败", e);
//        }

        return conf;
    }

    /**
     * 从HDFS读取Excel文件并解析为达标人群数据
     */
    private static Map<String, List<?>> readQualifiedDataFromHdfs(String hdfsFilePath) {
        FileSystem hdfs = null;
        InputStream inputStream = null;

        try {
            // 配置HDFS连接
            Configuration conf = buildHdfsConfiguration();
            hdfs = FileSystem.get(conf);

            Path filePath = new Path(hdfsFilePath);

            // 检查文件是否存在
            if (!hdfs.exists(filePath)) {
                logger.warn("HDFS文件不存在: {}", hdfsFilePath);
                return null;
            }

            logger.info("开始从HDFS读取Excel文件: {}", hdfsFilePath);

            // 打开文件流
            inputStream = hdfs.open(filePath);

            // 使用EasyExcelUtils解析达标人群数据
            Map<String, List<?>> excelData = EasyExcelUtils.readFFPQualifiedExcelFromStream(inputStream);

            logger.info("从HDFS文件读取完成，共 {} 个sheet页", excelData != null ? excelData.size() : 0);

            return excelData;

        } catch (Exception e) {
            logger.error("从HDFS读取Excel文件异常: {}", e.getMessage(), e);
            return null;
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception e) {
                    logger.warn("关闭HDFS输入流失败: {}", e.getMessage());
                }
            }
            if (hdfs != null) {
                try {
                    hdfs.close();
                    logger.info("HDFS连接已关闭");
                } catch (Exception e) {
                    logger.warn("关闭HDFS连接失败: {}", e.getMessage());
                }
            }
        }
    }

    /**
     * 处理"达标人群（报名）"数据
     */
    private static void processQualifiedRegistration(StreamExecutionEnvironment env,
                                                     Map<String, List<?>> excelData,
                                                     String etlDate) throws Exception {
        @SuppressWarnings("unchecked")
        List<TOdsClkQualifiedRegistration> dataList = (List<TOdsClkQualifiedRegistration>) excelData.get("达标人群（报名）");

        if (dataList == null || dataList.isEmpty()) {
            logger.warn("Sheet[达标人群（报名）]无数据，跳过处理");
            return;
        }

        logger.info("处理Sheet[达标人群（报名）]，数据条数: {}", dataList.size());

        DataStream<TOdsClkQualifiedRegistration> dataStream = env.addSource(
                new ExcelDataSource<>(dataList, etlDate),
                TypeInformation.of(TOdsClkQualifiedRegistration.class)
        ).name("Excel-Source-QualifiedRegistration")
        .map(record -> {
            // SM4加密MEMBER_CARD_NUMBER字段
            if (record.getMemberCardNumber() != null && !record.getMemberCardNumber().isEmpty()) {
                String encryptedCardNumber = SM4Utils.encrypt(record.getMemberCardNumber(), Constants.SM4_KEY);
                record.setMemberCardNumber(encryptedCardNumber);
            }
            return record;
        });

        DorisSink<TOdsClkQualifiedRegistration> dorisSink =
                FlinkDorisUtils.creatDorisODSSinkForFTP("T_ODS_CLK_QUALIFIED_REGISTRATION");

        dataStream.sinkTo(dorisSink).name("Doris-Sink-QualifiedRegistration");

        logger.info("Sheet[达标人群（报名）]数据流已配置完成");
    }

    /**
     * 处理"达标人群（无需报名）"数据
     */
    private static void processQualifiedNoRegistration(StreamExecutionEnvironment env,
                                                       Map<String, List<?>> excelData,
                                                       String etlDate) throws Exception {
        @SuppressWarnings("unchecked")
        List<TOdsClkQualifiedNoRegistration> dataList = (List<TOdsClkQualifiedNoRegistration>) excelData.get("达标人群（无需报名）");

        if (dataList == null || dataList.isEmpty()) {
            logger.warn("Sheet[达标人群（无需报名）]无数据，跳过处理");
            return;
        }

        logger.info("处理Sheet[达标人群（无需报名）]，数据条数: {}", dataList.size());

        DataStream<TOdsClkQualifiedNoRegistration> dataStream = env.addSource(
                new ExcelDataSource<>(dataList, etlDate),
                TypeInformation.of(TOdsClkQualifiedNoRegistration.class)
        ).name("Excel-Source-QualifiedNoRegistration")
        .map(record -> {
            // SM4加密MEMBER_CARD_NUMBER字段
            if (record.getMemberCardNumber() != null && !record.getMemberCardNumber().isEmpty()) {
                String encryptedCardNumber = SM4Utils.encrypt(record.getMemberCardNumber(), Constants.SM4_KEY);
                record.setMemberCardNumber(encryptedCardNumber);
            }
            return record;
        });

        DorisSink<TOdsClkQualifiedNoRegistration> dorisSink =
                FlinkDorisUtils.creatDorisODSSinkForFTP("T_ODS_CLK_QUALIFIED_NO_REGISTRATION");

        dataStream.sinkTo(dorisSink).name("Doris-Sink-QualifiedNoRegistration");

        logger.info("Sheet[达标人群（无需报名）]数据流已配置完成");
    }

    /**
     * Excel数据源实现（实现Serializable接口）
     * 将Excel中读取的数据转换为Flink DataStream
     *
     * @param <T> 实体类泛型
     */
    private static class ExcelDataSource<T> implements SourceFunction<T>, java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private final List<T> dataList;
        private final String etlDate;
        private volatile boolean isRunning = true;

        public ExcelDataSource(List<T> dataList, String etlDate) {
            this.dataList = dataList;
            this.etlDate = etlDate;
        }

        @Override
        public void run(SourceContext<T> ctx) throws Exception {
            for (T data : dataList) {
                if (!isRunning) {
                    break;
                }
                // 设置ETL字段
                setEtlFields(data, etlDate);
                ctx.collect(data);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        /**
         * 设置ETL相关字段
         */
        private void setEtlFields(T data, String etlDateStr) {
            try {
                // 当前时间（DATETIME(3)格式：包含三位毫秒精度）
                LocalDateTime now = LocalDateTime.now();
                String currentTimeStr = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));

                // ETL日期（DATE格式：仅日期）
                // etlDateStr 是 fileDate（yyyyMMdd格式）
                String sqlDateStr = LocalDate.parse(etlDateStr, DateTimeFormatter.ofPattern("yyyyMMdd"))
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

                // 生成ID（ETL_DATE + 序号，满足Doris聚合模型主键有序前缀要求）
                long sequenceNumber = dataList.indexOf(data) + 1;
                String idStr = etlDateStr + String.format("%010d", sequenceNumber);
                Long id = Long.parseLong(idStr);

                // 使用反射设置字段值
                setFieldValue(data, "id", id);
                setFieldValue(data, "etlCreateTime", currentTimeStr);
                setFieldValue(data, "etlUpdateTime", currentTimeStr);
                setFieldValue(data, "etlDate", sqlDateStr);

            } catch (Exception e) {
                logger.warn("设置ETL字段失败: {}", e.getMessage());
            }
        }

        /**
         * 使用反射设置字段值
         */
        private void setFieldValue(Object obj, String fieldName, Object value) {
            try {
                java.lang.reflect.Field field = obj.getClass().getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(obj, value);
            } catch (Exception e) {
                // 字段不存在时忽略
                logger.debug("字段{}不存在或设置失败: {}", fieldName, e.getMessage());
            }
        }
    }

    private static String convertToFileDateFormat(String dateStr) {
        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        DateTimeFormatter fileFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate date = LocalDate.parse(dateStr, inputFormatter);
        return date.format(fileFormatter);
    }
}
