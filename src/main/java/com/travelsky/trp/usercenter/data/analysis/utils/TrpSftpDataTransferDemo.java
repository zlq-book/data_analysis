package com.travelsky.trp.usercenter.data.analysis.utils;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.trp.TOdsScSaleDetail;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * TRP SFTP数据转储功能演示 - 基于Flink实现
 * 遵循项目规范：数据转储插入操作使用Flink执行，禁止使用原生JDBC方式进行数据插入
 * 
 * 数据流程：SFTP → HDFS → Flink → Doris
 * 1. 从SFTP服务器下载文件
 * 2. 将文件上传到项目的HDFS存储中
 * 3. 从HDFS读取文件数据流
 * 4. 使用Flink作业将数据写入Doris
 */
public class TrpSftpDataTransferDemo {

    private static final Logger logger = LoggerFactory.getLogger(TrpSftpDataTransferDemo.class);

    // 实体类与表名的映射关系
    private static final Map<Class<?>, String> ENTITY_TABLE_MAP = new HashMap<>();

    static {
        ENTITY_TABLE_MAP.put(TOdsScSaleDetail.class, "t_ods_sc_sale_detail");
        // 可以根据需要添加其他实体类与表名的映射
    }

    public static void main(String[] args) throws Exception {
        // SFTP模式：完整流程 SFTP → HDFS → Flink → Doris
        FTPConfigUtils.SftpConfig sftpConfig = new FTPConfigUtils.SftpConfig(
                Constants.TRP_FTP_HOST,
                Integer.parseInt(Constants.TRP_FTP_PORT),
                Constants.TRP_FTP_USERNAME,
                Constants.TRP_FTP_PWD,
                Constants.TRP_FTP_DOWNLOAD_PATH
        );
        processDataWithFlinkViaHdfs(sftpConfig, "sc_b2c_report_sale", TOdsScSaleDetail.class, "20251010");
    }

    /**
     * 使用Flink处理SFTP数据转储（经HDFS中转）
     * 流程：
     * 1. 从SFTP下载文件
     * 2. 上传到HDFS
     * 3. 从HDFS读取并用Flink处理
     * 4. 写入Doris
     *
     * @param sftpConfig    SFTP配置
     * @param filePrefix    文件前缀
     * @param entityClass   实体类
     * @param dateSignature 日期签名，为null时使用当前日期
     * @param <T>           实体类泛型
     */
    public static <T> void processDataWithFlinkViaHdfs(FTPConfigUtils.SftpConfig sftpConfig,
                                                       String filePrefix,
                                                       Class<T> entityClass,
                                                       String dateSignature) throws Exception {

        String currentDate = (dateSignature != null && !dateSignature.trim().isEmpty())
                ? dateSignature
                : LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        logger.info("开始处理SFTP数据转储（经HDFS）: {}_{}.*", filePrefix, currentDate);

        // 步骤1：从SFTP下载文件并上传到HDFS
        String hdfsFilePath = downloadFromSftpToHdfs(sftpConfig, filePrefix, currentDate);
        
        if (hdfsFilePath == null) {
            logger.error("无法从SFTP下载文件或上传到HDFS");
            return;
        }

        // 步骤2：从HDFS读取并使用Flink处理
        processDataFromHdfs(hdfsFilePath, entityClass, currentDate, filePrefix);

        logger.info("SFTP数据转储完成（经HDFS）: {}", filePrefix);
    }

    /**
     * 从SFTP下载文件并上传到HDFS
     *
     * @param sftpConfig SFTP配置
     * @param filePrefix 文件前缀
     * @param dateSignature 日期签名
     * @return HDFS文件路径，失败返回null
     */
    private static String downloadFromSftpToHdfs(FTPConfigUtils.SftpConfig sftpConfig,
                                                 String filePrefix,
                                                 String dateSignature) {
        SFTPUtils sftpUtils = null;
        FileSystem hdfs = null;
        InputStream sftpInputStream = null;
        
        try {
            // 1. 连接SFTP
            sftpUtils = new SFTPUtils(sftpConfig.getHost(), sftpConfig.getPort(),
                    sftpConfig.getUsername(), sftpConfig.getPassword());
            
            if (!sftpUtils.connect()) {
                throw new RuntimeException("SFTP连接失败");
            }
            
            logger.info("SFTP连接成功: {}:{}", sftpConfig.getHost(), sftpConfig.getPort());
            
            // 2. 切换到目标目录
            if (sftpConfig.getRemoteDirectory() != null && !sftpConfig.getRemoteDirectory().isEmpty()) {
                if (!sftpUtils.changeWorkingDirectory(sftpConfig.getRemoteDirectory())) {
                    throw new RuntimeException("无法切换到目标目录: " + sftpConfig.getRemoteDirectory());
                }
            }
            
            // 3. 智能文件查找
            String fileName = findAvailableFile(sftpUtils, filePrefix, dateSignature);
            if (fileName == null) {
                logger.warn("未找到匹配的文件: {}_{}", filePrefix, dateSignature);
                return null;
            }
            
            logger.info("找到SFTP文件: {}", fileName);
            
            // 4. 获取SFTP文件流
            sftpInputStream = sftpUtils.getFileInputStream(fileName);
            if (sftpInputStream == null) {
                throw new RuntimeException("无法获取SFTP文件流: " + fileName);
            }
            
            // 5. 配置HDFS连接
            Configuration hdfsConf = buildHdfsConfiguration();
            hdfs = FileSystem.get(hdfsConf);
            
            // 6. 构建HDFS目标路径（按日期分区）
            String hdfsBaseDir = "/data/trp/sftp_transfer";
            String hdfsDir = hdfsBaseDir + "/" + dateSignature;
            String hdfsFilePath = hdfsDir + "/" + fileName;
            
            Path hdfsPath = new Path(hdfsFilePath);
            Path hdfsDirPath = new Path(hdfsDir);
            
            // 7. 创建HDFS目录（如果不存在）
            if (!hdfs.exists(hdfsDirPath)) {
                hdfs.mkdirs(hdfsDirPath);
                logger.info("创建HDFS目录: {}", hdfsDir);
            }
            
            // 8. 上传文件到HDFS（如果已存在则覆盖）
            if (hdfs.exists(hdfsPath)) {
                hdfs.delete(hdfsPath, false);
                logger.info("删除已存在的HDFS文件: {}", hdfsFilePath);
            }
            
            org.apache.hadoop.fs.FSDataOutputStream hdfsOutputStream = hdfs.create(hdfsPath);
            
            // 9. 从 SFTP 流复制到 HDFS 流
            byte[] buffer = new byte[8192];
            int bytesRead;
            long totalBytes = 0;
            
            while ((bytesRead = sftpInputStream.read(buffer)) != -1) {
                hdfsOutputStream.write(buffer, 0, bytesRead);
                totalBytes += bytesRead;
            }
            
            hdfsOutputStream.close();
            logger.info("文件上传到HDFS成功: {} (大小: {} 字节)", hdfsFilePath, totalBytes);
            
            return hdfsFilePath;
            
        } catch (Exception e) {
            logger.error("从SFTP下载并上传到HDFS失败: {}", e.getMessage(), e);
            return null;
        } finally {
            // 关闭资源
            if (sftpInputStream != null) {
                try {
                    sftpInputStream.close();
                } catch (Exception e) {
                    logger.warn("关闭SFTP输入流失败: {}", e.getMessage());
                }
            }
            if (sftpUtils != null) {
                sftpUtils.disconnect();
            }
            if (hdfs != null) {
                try {
                    hdfs.close();
                } catch (Exception e) {
                    logger.warn("关闭HDFS连接失败: {}", e.getMessage());
                }
            }
        }
    }

    /**
     * 从HDFS读取文件并使用Flink处理
     *
     * @param hdfsFilePath HDFS文件路径
     * @param entityClass 实体类
     * @param dateSignature 日期签名
     * @param filePrefix 文件前缀（用于命名）
     * @param <T> 实体类泛型
     */
    private static <T> void processDataFromHdfs(String hdfsFilePath,
                                                Class<T> entityClass,
                                                String dateSignature,
                                                String filePrefix) throws Exception {
        // 1. 创建Flink执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置checkpoint
        CheckpointUtils.setCheckpoint(env, "TrpHdfsDataTransfer-" + filePrefix + "-" + dateSignature);

        logger.info("开始从HDFS读取文件: {}", hdfsFilePath);

        // 2. 从HDFS读取数据并创建数据流（显式指定类型信息）
        TypeInformation<T> typeInfo = TypeExtractor.createTypeInfo(entityClass);
        DataStream<T> dataStream = env.addSource(
                new HdfsFileSource<>(hdfsFilePath, entityClass)
        ).returns(typeInfo).name("HDFS-Source-" + filePrefix);

        // 3. 数据处理和清洗（使用匿名类避免类型擦除问题）
        DataStream<T> processedStream = dataStream
                .flatMap(new FlatMapFunction<T, T>() {
                    private int debugCounter = 0;  // 调试计数器

                    @Override
                    public void flatMap(T value, Collector<T> out) throws Exception {
                        if (value != null) {
                            // 设置ETL字段
                            setEtlFields(value, dateSignature);

                            // 打印前3条数据的JSON格式，用于调试
                            if (debugCounter < 3) {
                                try {
                                    com.fasterxml.jackson.databind.ObjectMapper mapper =
                                            new com.fasterxml.jackson.databind.ObjectMapper();
                                    mapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
                                    String json = mapper.writeValueAsString(value);
                                    logger.info("示例数据 JSON ({}): {}", debugCounter + 1, json);
                                    debugCounter++;
                                } catch (Exception e) {
                                    logger.warn("序列化调试信息失败: {}", e.getMessage());
                                }
                            }

                            out.collect(value);
                        }
                    }
                })
                .returns(typeInfo)  // 显式指定类型信息解决类型擦除问题
                .name("ETL-Process-" + filePrefix);

        // 4. 创建Doris Sink并写入数据（使用Flink而非JDBC）
        String tableName = getTableNameByEntity(entityClass).toUpperCase();
        DorisSink<T> dorisSink = FlinkDorisUtils.creatDorisODSSinkForFTP(tableName);

        // 5. 将数据写入Doris
        processedStream.sinkTo(dorisSink).name("Doris-Sink-" + tableName);

        // 6. 执行Flink作业
        env.execute("TRP-HDFS-DataTransfer-" + filePrefix + "-" + dateSignature);

        logger.info("Flink作业执行完成: {}", filePrefix);
    }

    /**
     * 智能文件查找：按照优先级查找可用的文件格式
     */
    private static String findAvailableFile(SFTPUtils sftpUtils, String filePrefix, String dateSignature) {
        String[] extensions = {".xlsx", ".xls", ".csv"};
        
        for (String ext : extensions) {
            String fileName = filePrefix + "_" + dateSignature + ext;
            if (sftpUtils.fileExists(fileName)) {
                logger.info("找到文件: {} (格式: {})", fileName, ext);
                return fileName;
            }
        }
        
        logger.warn("未找到任何匹配的文件: {}_{}.{xlsx,xls,csv}", filePrefix, dateSignature);
        return null;
    }

    /**
     * 构建HDFS配置
     */
    private static Configuration buildHdfsConfiguration() {
        System.setProperty("java.security.krb5.conf", Constants.KRB5_CONF_PATH);
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://cmss");
        conf.set("dfs.nameservices", "cmss");
        conf.set("dfs.ha.namenodes.cmss", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.cmss.nn1", "emr-master-001.novalocal:8020");
        conf.set("dfs.namenode.rpc-address.cmss.nn2", "emr-master-ha-001.novalocal:8020");
        conf.set("dfs.client.failover.proxy.provider.cmss",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        return conf;
    }

    /**
     * 设置ETL相关字段
     * 注意：使用Doris兼容的字符串格式
     * DATETIME(3): "yyyy-MM-dd HH:mm:ss.SSS"
     * DATE: "yyyy-MM-dd"
     */
    private static <T> void setEtlFields(T data, String etlDate) {
        try {
            // DATETIME(3)格式：必须包含三位毫秒精度!
            LocalDateTime now = LocalDateTime.now();
            String currentTimeStr = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));

            // DATE格式：仅日期
            String sqlDateStr = LocalDate.parse(etlDate, DateTimeFormatter.ofPattern("yyyyMMdd"))
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

            // 使用反射设置ETL字段为字符串格式
            setFieldValue(data, "etlCreateTime", currentTimeStr);
            setFieldValue(data, "etlUpdateTime", currentTimeStr);
            setFieldValue(data, "etlDate", sqlDateStr);

            logger.debug("设置ETL字段 - createTime: {}, updateTime: {}, date: {}",
                    currentTimeStr, currentTimeStr, sqlDateStr);
        } catch (Exception e) {
            logger.warn("设置ETL字段失败: {}", e.getMessage());
        }
    }

    /**
     * 使用反射设置字段值
     */
    private static void setFieldValue(Object obj, String fieldName, Object value) {
        try {
            java.lang.reflect.Field field = obj.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(obj, value);
        } catch (Exception e) {
            // 字段不存在时忽略
        }
    }

    /**
     * 根据实体类获取对应的表名
     */
    private static String getTableNameByEntity(Class<?> entityClass) {
        return ENTITY_TABLE_MAP.getOrDefault(entityClass, "unknown_table");
    }
}