package com.travelsky.trp.usercenter.data.analysis.utils;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.rate.TOdsSccrmExchangeRateDetail;
import com.travelsky.dataplatform.module.ods.trp.*;
import com.travelsky.dataplatform.module.ods.yxw.TOdsYxwSaleReport;
import com.travelsky.dataplatform.udf.DocumentNumberNormalizeUDF;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.udf.TrpAESDecryptor;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.KerberosAuthUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TRP Flink数据转储工具类
 * 支持SFTP → HDFS → Flink → Doris的完整数据流转
 * 支持两种数据处理模式：
 * 1. DataStream API模式：直接使用DataStream处理（适用于简单场景）
 * 2. Table API/SQL模式：使用SQL + UDF自动化处理加解密（推荐，更优雅）
 * 用于生产环境的数据转储任务
 */
public class TrpFlinkTransferUtils {

    private static final Logger logger = LoggerFactory.getLogger(TrpFlinkTransferUtils.class);

    // 实体类与表名的映射关系
    private static final Map<Class<?>, String> ENTITY_TABLE_MAP = new HashMap<>();

    static {
        ENTITY_TABLE_MAP.put(TOdsScSaleDetail.class, "t_ods_trp_sc_sale_detail");
        ENTITY_TABLE_MAP.put(TOdsScAirRefundDetail.class, "t_ods_trp_air_refund_detail");
        ENTITY_TABLE_MAP.put(TOdsScInsuranceSaleDetail.class, "t_ods_trp_insurance_sale_detail");
        ENTITY_TABLE_MAP.put(TOdsScInsuranceRefundDetail.class, "t_ods_trp_insurance_refund_detail");
        ENTITY_TABLE_MAP.put(TOdsScCouponSaleDetail.class, "t_ods_trp_coupon_sale_detail");
        ENTITY_TABLE_MAP.put(TOdsScCouponRefundDetail.class, "t_ods_trp_coupon_refund_detail");
        ENTITY_TABLE_MAP.put(TOdsYxwSaleReport.class, "t_ods_yxw_sale_report");
        ENTITY_TABLE_MAP.put(TOdsSccrmExchangeRateDetail.class, "T_ODS_SCCRM_EXCHANGE_RATE");
    }

    /**
     * 从SFTP下载文件并上传到HDFS（公共方法，支持外部调用）
     *
     * @param sftpConfig    SFTP配置
     * @param filePrefix    文件前缀
     * @param dateSignature 日期签名
     * @return HDFS文件路径，文件不存在时返回null
     * @throws RuntimeException 当发生连接失败、传输失败等错误时抛出异常
     */
    public static String downloadFromSftpToHdfs(FTPConfigUtils.SftpConfig sftpConfig,
                                                String filePrefix,
                                                String dateSignature) {
        return downloadFromSftpToHdfs(sftpConfig, filePrefix, dateSignature, null);
    }

    /**
     * 从SFTP下载文件并上传到HDFS（支持自定义HDFS基础目录）
     *
     * @param sftpConfig    SFTP配置
     * @param filePrefix    文件前缀
     * @param dateSignature 日期签名
     * @param hdfsBaseDir   HDFS基础目录（为null时使用默认TRP_FTP_HDFS）
     * @return HDFS文件路径，文件不存在时返回null
     * @throws RuntimeException 当发生连接失败、传输失败等错误时抛出异常
     */
    public static String downloadFromSftpToHdfs(FTPConfigUtils.SftpConfig sftpConfig,
                                                String filePrefix,
                                                String dateSignature,
                                                String hdfsBaseDir) {
        SFTPUtils sftpUtils = null;
        FileSystem hdfs = null;
        InputStream sftpInputStream = null;
        org.apache.hadoop.fs.FSDataOutputStream hdfsOutputStream = null;

        try {
            // 1. 先配置HDFS连接和Kerberos认证
            Configuration hdfsConf = buildHdfsConfiguration();
            hdfs = FileSystem.get(hdfsConf);
            logger.info("HDFS连接成功");

            // 2. 连接SFTP
            sftpUtils = new SFTPUtils(sftpConfig.getHost(), sftpConfig.getPort(),
                    sftpConfig.getUsername(), sftpConfig.getPassword());

            if (!sftpUtils.connect()) {
                throw new RuntimeException("SFTP连接失败");
            }

            logger.info("SFTP连接成功: {}:{}", sftpConfig.getHost(), sftpConfig.getPort());

            // 3. 切换到目标目录
            if (sftpConfig.getRemoteDirectory() != null && !sftpConfig.getRemoteDirectory().isEmpty()) {
                if (!sftpUtils.changeWorkingDirectory(sftpConfig.getRemoteDirectory())) {
                    throw new RuntimeException("无法切换到目标目录: " + sftpConfig.getRemoteDirectory());
                }
            }

            // 4. 智能文件查找
            String fileName = findAvailableFile(sftpUtils, filePrefix, dateSignature);
            if (fileName == null) {
                logger.warn("未找到匹配的文件: {}_{}", filePrefix, dateSignature);
                return null;
            }

            logger.info("找到SFTP文件: {}", fileName);
            
            // 5. 获取SFTP文件大小信息
            try {
                com.jcraft.jsch.SftpATTRS attrs = sftpUtils.getSftpChannel().stat(fileName);
                long remoteFileSize = attrs.getSize();
                logger.info("SFTP远程文件大小: {} 字节 ({} MB)", remoteFileSize, remoteFileSize / 1024.0 / 1024.0);
                
                if (remoteFileSize == 0) {
                    logger.error("错误: SFTP文件大小为0，文件可能为空或不完整");
                    return null;
                } else if (remoteFileSize < 1000) {
                    logger.warn("警告: SFTP文件大小只有 {} 字节，可能不是完整的数据文件", remoteFileSize);
                }
            } catch (Exception e) {
                logger.warn("无法获取SFTP文件大小: {}", e.getMessage());
            }

            // 6. 构建HDFS目标路径（按日期分区）
            // 如果未指定hdfsBaseDir，则根据文件前缀智能判断
            String finalHdfsBaseDir;
            if (hdfsBaseDir != null && !hdfsBaseDir.isEmpty()) {
                finalHdfsBaseDir = hdfsBaseDir;
            } else if (filePrefix.startsWith("YXW_")) {
                finalHdfsBaseDir = Constants.YXW_FTP_HDFS;
                logger.info("检测到YXW数据源，使用YXW_FTP_HDFS: {}", finalHdfsBaseDir);
            } else {
                finalHdfsBaseDir = Constants.TRP_FTP_HDFS;
            }

            String hdfsDir = finalHdfsBaseDir + "/" + dateSignature;
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

            // 9. 获取SFTP文件流（在创建HDFS输出流之前）
            sftpInputStream = sftpUtils.getFileInputStream(fileName);
            if (sftpInputStream == null) {
                throw new RuntimeException("无法获取SFTP文件流: " + fileName);
            }

            // 10. 创建HDFS输出流
            hdfsOutputStream = hdfs.create(hdfsPath, true);
            logger.info("开始传输文件到HDFS: {}", hdfsFilePath);

            // 11. 从 SFTP 流复制到 HDFS 流（使用更大的缓冲区）
            byte[] buffer = new byte[65536]; // 64KB缓冲区，提高传输效率
            int bytesRead;
            long totalBytes = 0;
            int chunkCount = 0;

            while ((bytesRead = sftpInputStream.read(buffer)) != -1) {
                hdfsOutputStream.write(buffer, 0, bytesRead);
                totalBytes += bytesRead;
            }

            // 12. 确保数据完全刷新到HDFS
            hdfsOutputStream.hflush();
            hdfsOutputStream.hsync();
            hdfsOutputStream.close();
            hdfsOutputStream = null; // 标记为已关闭
            
            logger.info("文件上传到HDFS成功: {} ", hdfsFilePath);

            return hdfsFilePath;

        } catch (Exception e) {
            logger.error("从SFTP下载并上传到HDFS失败: {}", e.getMessage(), e);
            throw new RuntimeException("从SFTP下载并上传到HDFS失败: " + e.getMessage(), e);
        } finally {
            // 关闭资源（按照打开的相反顺序关闭）
            if (hdfsOutputStream != null) {
                try {
                    hdfsOutputStream.close();
                    logger.info("HDFS输出流已关闭");
                } catch (Exception e) {
                    logger.warn("关闭HDFS输出流失败: {}", e.getMessage());
                }
            }
            if (sftpInputStream != null) {
                try {
                    sftpInputStream.close();
                    logger.info("SFTP输入流已关闭");
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
                    logger.info("HDFS连接已关闭");
                } catch (Exception e) {
                    logger.warn("关闭HDFS连接失败: {}", e.getMessage());
                }
            }
        }
    }

    /**
     * 批量从SFTP下载多个日期文件到HDFS（并发，复用HDFS和SFTP连接）
     * 优化：使用SFTP连接池，避免每个文件都建立和断开连接
     *
     * @param sftpConfig    SFTP配置
     * @param filePrefix    文件前缀
     * @param dateList      日期列表
     * @param maxConcurrency 最大并发数
     * @return 成功下载的文件信息列表
     * @throws RuntimeException 当初始化失败（如HDFS连接失败、SFTP连接池创建失败等）时抛出异常
     */
    public static List<HdfsFileInfo> downloadBatchFromSftpToHdfs(FTPConfigUtils.SftpConfig sftpConfig,
                                                                 String filePrefix,
                                                                 List<String> dateList,
                                                                 int maxConcurrency) {
        if (dateList == null || dateList.isEmpty()) {
            return new ArrayList<>();
        }

        List<HdfsFileInfo> results = new ArrayList<>();
        java.util.concurrent.ConcurrentLinkedQueue<HdfsFileInfo> successQueue = new java.util.concurrent.ConcurrentLinkedQueue<>();
        java.util.concurrent.ConcurrentLinkedQueue<String> failedDates = new java.util.concurrent.ConcurrentLinkedQueue<>();

        org.apache.hadoop.fs.FileSystem sharedHdfs = null;
        java.util.concurrent.BlockingQueue<SFTPUtils> sftpPool = new java.util.concurrent.LinkedBlockingQueue<>();
        List<SFTPUtils> allSftpConnections = new ArrayList<>();

        try {
            Configuration conf = buildHdfsConfiguration();
            sharedHdfs = FileSystem.get(conf);

            // 优化：预先创建SFTP连接池，每个线程一个连接，复用连接
            int threads = Math.max(1, maxConcurrency);
            logger.info("创建SFTP连接池，连接数: {}", threads);
            for (int i = 0; i < threads; i++) {
                SFTPUtils sftpUtils = new SFTPUtils(sftpConfig.getHost(), sftpConfig.getPort(),
                        sftpConfig.getUsername(), sftpConfig.getPassword());
                if (sftpUtils.connect()) {
                    if (sftpConfig.getRemoteDirectory() != null && !sftpConfig.getRemoteDirectory().isEmpty()) {
                        if (!sftpUtils.changeWorkingDirectory(sftpConfig.getRemoteDirectory())) {
                            logger.warn("无法切换到目标目录: {}, 连接将被关闭", sftpConfig.getRemoteDirectory());
                            sftpUtils.disconnect();
                            continue;
                        }
                    }
                    sftpPool.offer(sftpUtils);
                    allSftpConnections.add(sftpUtils);
                } else {
                    logger.warn("SFTP连接池创建连接失败: {}/{}", i + 1, threads);
                }
            }

            if (sftpPool.isEmpty()) {
                logger.error("SFTP连接池创建失败，无法继续下载");
                throw new RuntimeException("SFTP连接池创建失败，所有连接尝试均失败，无法继续下载");
            }

            logger.info("SFTP连接池创建成功，可用连接数: {}", sftpPool.size());

            java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
            List<java.util.concurrent.Future<?>> futures = new ArrayList<>();

            for (String date : dateList) {
                FileSystem finalSharedHdfs = sharedHdfs;
                futures.add(pool.submit(() -> {
                    SFTPUtils sftpUtils = null;
                    try {
                        // 从连接池获取连接
                        sftpUtils = sftpPool.take();
                        String hdfsPath = downloadSingleWithSharedSftp(finalSharedHdfs, sftpUtils, 
                                sftpConfig, filePrefix, date, null);
                        if (hdfsPath != null) {
                            successQueue.add(new HdfsFileInfo(hdfsPath, date));
                        } else {
                            failedDates.add(date);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        logger.error("获取SFTP连接被中断: {}", e.getMessage());
                        failedDates.add(date);
                    } catch (Exception e) {
                        logger.error("下载文件失败: {}", e.getMessage(), e);
                        failedDates.add(date);
                    } finally {
                        // 归还连接到池中
                        if (sftpUtils != null && sftpUtils.isConnected()) {
                            sftpPool.offer(sftpUtils);
                        }
                    }
                }));
            }

            for (java.util.concurrent.Future<?> f : futures) {
                try {
                    f.get();
                } catch (Exception e) {
                    // 忽略，已在子任务中记录
                }
            }

            pool.shutdown();
            try {
                if (!pool.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS)) {
                    pool.shutdownNow();
                }
            } catch (InterruptedException e) {
                pool.shutdownNow();
                Thread.currentThread().interrupt();
            }

        } catch (Exception e) {
            logger.error("批量下载初始化失败: {}", e.getMessage(), e);
            throw new RuntimeException("批量下载初始化失败: " + e.getMessage(), e);
        } finally {
            // 关闭所有SFTP连接
            for (SFTPUtils sftp : allSftpConnections) {
                try {
                    if (sftp != null && sftp.isConnected()) {
                        sftp.disconnect();
                    }
                } catch (Exception ignore) {}
            }
            logger.info("所有SFTP连接已关闭");

            if (sharedHdfs != null) {
                try {
                    sharedHdfs.close();
                } catch (Exception ignore) {}
            }
        }

        results.addAll(successQueue);
        if (!failedDates.isEmpty()) {
            logger.warn("批量下载失败日期: {}", failedDates);
        }
        logger.info("批量下载完成，成功: {}, 失败: {}", results.size(), failedDates.size());
        return results;
    }

    /**
     * 使用共享的SFTP连接下载单个文件（不复用连接，已废弃）
     * 保留此方法以兼容旧代码
     */
    @Deprecated
    private static String downloadSingleWithSharedHdfs(FileSystem hdfs,
                                                       FTPConfigUtils.SftpConfig sftpConfig,
                                                       String filePrefix,
                                                       String dateSignature,
                                                       String hdfsBaseDir) {
        SFTPUtils sftpUtils = null;
        try {
            sftpUtils = new SFTPUtils(sftpConfig.getHost(), sftpConfig.getPort(),
                    sftpConfig.getUsername(), sftpConfig.getPassword());
            if (!sftpUtils.connect()) {
                throw new RuntimeException("SFTP连接失败");
            }
            if (sftpConfig.getRemoteDirectory() != null && !sftpConfig.getRemoteDirectory().isEmpty()) {
                if (!sftpUtils.changeWorkingDirectory(sftpConfig.getRemoteDirectory())) {
                    throw new RuntimeException("无法切换到目标目录: " + sftpConfig.getRemoteDirectory());
                }
            }
            return downloadSingleWithSharedSftp(hdfs, sftpUtils, sftpConfig, filePrefix, dateSignature, hdfsBaseDir);
        } catch (Exception e) {
            logger.error("下载失败: {}", e.getMessage(), e);
            throw new RuntimeException("下载失败: " + e.getMessage(), e);
        } finally {
            if (sftpUtils != null) {
                sftpUtils.disconnect();
            }
        }
    }

    /**
     * 使用共享的SFTP连接下载单个文件（复用连接）
     * 优化：不复用连接，连接由连接池管理
     */
    private static String downloadSingleWithSharedSftp(FileSystem hdfs,
                                                        SFTPUtils sftpUtils,
                                                        FTPConfigUtils.SftpConfig sftpConfig,
                                                        String filePrefix,
                                                        String dateSignature,
                                                        String hdfsBaseDir) {
        InputStream sftpInputStream = null;
        org.apache.hadoop.fs.FSDataOutputStream hdfsOutputStream = null;
        try {
            // 检查SFTP连接是否有效
            if (sftpUtils == null || !sftpUtils.isConnected()) {
                throw new RuntimeException("SFTP连接无效或已断开");
            }

            String fileName = findAvailableFile(sftpUtils, filePrefix, dateSignature);
            if (fileName == null) {
                logger.warn("未找到匹配的文件: {}_{}", filePrefix, dateSignature);
                return null;
            }

            String finalHdfsBaseDir;
            if (hdfsBaseDir != null && !hdfsBaseDir.isEmpty()) {
                finalHdfsBaseDir = hdfsBaseDir;
            } else if (filePrefix.startsWith("YXW_")) {
                finalHdfsBaseDir = Constants.YXW_FTP_HDFS;
            } else {
                finalHdfsBaseDir = Constants.TRP_FTP_HDFS;
            }

            String hdfsDir = finalHdfsBaseDir + "/" + dateSignature;
            String hdfsFilePath = hdfsDir + "/" + fileName;
            Path hdfsPath = new Path(hdfsFilePath);
            Path hdfsDirPath = new Path(hdfsDir);

            synchronized (hdfs) {
                if (!hdfs.exists(hdfsDirPath)) {
                    hdfs.mkdirs(hdfsDirPath);
                }
                if (hdfs.exists(hdfsPath)) {
                    hdfs.delete(hdfsPath, false);
                }
            }

            sftpInputStream = sftpUtils.getFileInputStream(fileName);
            if (sftpInputStream == null) {
                throw new RuntimeException("无法获取SFTP文件流: " + fileName);
            }

            hdfsOutputStream = hdfs.create(hdfsPath, true);
            byte[] buffer = new byte[65536];
            int bytesRead;
            long totalBytes = 0;
            while ((bytesRead = sftpInputStream.read(buffer)) != -1) {
                hdfsOutputStream.write(buffer, 0, bytesRead);
                totalBytes += bytesRead;
            }
            hdfsOutputStream.hflush();
            hdfsOutputStream.hsync();
            hdfsOutputStream.close();
            hdfsOutputStream = null;

            logger.info("文件下载成功: {} (大小: {} MB)", hdfsFilePath, totalBytes / 1024 / 1024);
            return hdfsFilePath;
        } catch (Exception e) {
            logger.error("下载文件失败: 日期={}, 错误: {}", dateSignature, e.getMessage(), e);
            throw new RuntimeException("下载文件失败: 日期=" + dateSignature + ", 错误: " + e.getMessage(), e);
        } finally {
            if (hdfsOutputStream != null) {
                try { hdfsOutputStream.close(); } catch (Exception ignore) {}
            }
            if (sftpInputStream != null) {
                try { sftpInputStream.close(); } catch (Exception ignore) {}
            }
            // 注意：不复用连接，连接由连接池管理，在调用方归还
        }
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
        
        // 使用KerberosAuthUtils工具类进行Kerberos认证
//        try {
//            KerberosAuthUtils.initKerberosAuth(
//                Constants.KRB5_CONF_PATH,
//                Constants.SYS_UCV_TEST_FOR_MAPREDUCE_PRINCIPAL,
//                Constants.SYS_UCV_TEST_FOR_MAPREDUCE_KEYTAB_PATH
//            );
//            logger.info("Kerberos认证成功（使用KerberosAuthUtils工具类）: {}", Constants.SYS_UCV_TEST_FOR_MAPREDUCE_PRINCIPAL);
//        } catch (Exception e) {
//            logger.error("Kerberos认证失败: {}", e.getMessage(), e);
//            throw new RuntimeException("Kerberos认证失败", e);
//        }
        
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
     * 【推荐】从HDFS读取文件并使用Table API/SQL处理（支持自动化字段加解密）
     * 使用SQL + UDF方式，更优雅、更高效
     * 注意：使用toChangelogStream避免BigDecimal类型推断问题
     *
     * @param hdfsFilePath      HDFS文件路径
     * @param entityClass       实体类
     * @param dateSignature     日期签名
     * @param filePrefix        文件前缀（用于命名）
     * @param encryptConfig     加密字段配置（可选，为null则不处理加密）
     * @param <T>               实体类泛型
     * @throws Exception        处理异常
     */
    public static <T> void processDataFromHdfsWithSQL(String hdfsFilePath,
                                                       Class<T> entityClass,
                                                       String dateSignature,
                                                       String filePrefix,
                                                       EncryptFieldConfig encryptConfig) throws Exception {
        if (hdfsFilePath == null || hdfsFilePath.trim().isEmpty()) {
            throw new IllegalArgumentException("HDFS文件路径不能为空");
        }
        if (entityClass == null) {
            throw new IllegalArgumentException("实体类不能为空");
        }
        
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置checkpoint
        CheckpointUtils.setCheckpoint(env, "TrpHdfsDataTransfer-SQL-" + filePrefix + "-" + dateSignature);

        // 2. 创建Table环境
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 3. 注册UDF函数(如果需要加解密)
        if (encryptConfig != null && encryptConfig.hasEncryptFields()) {
            tEnv.createTemporarySystemFunction("aes_decrypt", TrpAESDecryptor.class);
            tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
            tEnv.createTemporarySystemFunction("normalize_document", DocumentNumberNormalizeUDF.class);
            logger.info("已注册AES解密、SM4加密和证件号标准化UDF函数");
        }

        logger.info("开始从HDFS读取文件: {}", hdfsFilePath);

        // 4. 从HDFS读取数据并创建临时视图
        TypeInformation<T> typeInfo = TypeExtractor.createTypeInfo(entityClass);
        DataStream<T> dataStream;

        // 根据不同的实体类配置不同的文件解析参数
        // 注意：CSV文件会自动检测编码（GBK或UTF-8），配置的编码仅作为默认值
        if (entityClass == TOdsYxwSaleReport.class) {
            // YXW销售报表：特殊分隔符'↑'，UTF-8编码（默认值，实际会自动检测），表头行数为1
            logger.info("配置YXW销售报表解析参数: 分隔符='↑', 编码=UTF-8(默认，将自动检测), 表头行数=1");
            dataStream = env.addSource(
                    new HdfsFileSource<>(hdfsFilePath, entityClass, '↑', StandardCharsets.UTF_8, 1)
            ).returns(typeInfo).name("HDFS-Source-" + filePrefix);
            
        } else if (entityClass == TOdsSccrmExchangeRateDetail.class) {
            // 汇率表：Excel文件，分隔符参数不生效，但配置GBK编码和表头行数为1
            // 自定义ID生成逻辑：日期签名 + 货币代号
            logger.info("配置汇率表解析参数: xlsx, 自定义ID处理器");
            dataStream = env.addSource(
                    new HdfsFileSource<>(hdfsFilePath, entityClass, createExchangeRateIdProcessor(dateSignature))
            ).returns(typeInfo).name("HDFS-Source-" + filePrefix);
            
        } else {
            // 默认配置：逗号分隔符，GBK编码，表头行数为1
            logger.info("使用默认解析参数: 分隔符=',', 编码=GBK, 表头行数=1");
            dataStream = env.addSource(
                    new HdfsFileSource<>(hdfsFilePath, entityClass, ',', Charset.forName("GBK"), 2)
            ).returns(typeInfo).name("HDFS-Source-" + filePrefix);
        }


        // 5. 将DataStream转换为Table
        Table sourceTable = tEnv.fromDataStream(dataStream);
        tEnv.createTemporaryView("SOURCE_TABLE", sourceTable);

        // 6. 构建SQL查询语句（包含加解密逻辑）
        String selectSql = buildSelectSql(entityClass, encryptConfig);
        logger.info("执行SQL查询: {}", selectSql);

        // 7. 执行SQL查询
        Table resultTable = tEnv.sqlQuery(selectSql);

        // 8. 将结果表转为ChangelogStream（避免BigDecimal类型推断问题）
        DataStream<org.apache.flink.types.Row> rowStream = tEnv.toChangelogStream(resultTable);

        // 9. 将Row转换为实体类，并添加ETL字段
        DataStream<T> processedStream = rowStream
                .map(row -> rowToEntity(row, entityClass, dateSignature))
                .returns(typeInfo)
                .name("Row-To-Entity-" + filePrefix);

        // 10. 创建Doris Sink并写入数据
        String tableName = getTableNameByEntity(entityClass).toUpperCase();
        DorisSink<T> dorisSink = FlinkDorisUtils.creatDorisODSSinkForFTP(tableName);
        // 11. 将数据写入Doris
        processedStream.sinkTo(dorisSink).name("Doris-Sink-" + tableName);

        // 12. 执行Flink作业
        env.execute("TRP-HDFS-SQL-DataTransfer-" + filePrefix + "-" + dateSignature);

        logger.info("Flink作业执行完成: {}", filePrefix);
    }

    /**
     * 将Row转换为实体类对象
     */
    private static <T> T rowToEntity(org.apache.flink.types.Row row, Class<T> entityClass, String dateSignature) {
        try {
            T entity = entityClass.newInstance();
            
            // 获取所有字段
            java.lang.reflect.Field[] fields = entityClass.getDeclaredFields();
            int fieldIndex = 0;
            
            for (java.lang.reflect.Field field : fields) {
                // 跳过静态字段
                if (java.lang.reflect.Modifier.isStatic(field.getModifiers()) || 
                    "serialVersionUID".equals(field.getName())) {
                    continue;
                }
                
                field.setAccessible(true);
                Object value = row.getField(fieldIndex);
                
                if (value != null) {
                    field.set(entity, value);
                }
                
                fieldIndex++;
            }
            
            // 设置ETL字段
            setEtlFields(entity, dateSignature);
            
            return entity;
        } catch (Exception e) {
            logger.error("Row转实体失败: {}", e.getMessage(), e);
            throw new RuntimeException("Row转实体失败", e);
        }
    }

    /**
     * 构建SQL查询语句（包含字段加解密逻辑）
     * 注意：ID字段已在HdfsFileSource中设置完成（etlDate + 序号），此处直接使用
     */
    private static <T> String buildSelectSql(Class<T> entityClass,
                                              EncryptFieldConfig encryptConfig) {
        StringBuilder sql = new StringBuilder("SELECT ");

        // SQL保留关键字列表
        java.util.Set<String> reservedKeywords = new java.util.HashSet<>(java.util.Arrays.asList(
            "language", "user", "order", "position", "timestamp", "date", "time",
            "year", "month", "day", "hour", "minute", "second", "interval",
            "value", "key", "rank", "row", "column", "table", "database", "schema"
        ));

        // 获取所有字段
        java.lang.reflect.Field[] fields = entityClass.getDeclaredFields();
        boolean firstField = true;

        for (java.lang.reflect.Field field : fields) {
            String fieldName = field.getName();

            // 跳过静态字段和serialVersionUID
            if (java.lang.reflect.Modifier.isStatic(field.getModifiers()) || 
                "serialVersionUID".equals(fieldName)) {
                continue;
            }

            if (!firstField) {
                sql.append(", ");
            }
            firstField = false;

            // 处理SQL保留关键字
            String quotedFieldName = reservedKeywords.contains(fieldName.toLowerCase()) 
                ? "`" + fieldName + "`" 
                : fieldName;

            // 检查是否需要加解密
            if (encryptConfig != null) {
                EncryptFieldConfig.EncryptMode mode = encryptConfig.getEncryptMode(fieldName);
                if (mode != null) {
                    switch (mode) {
                        case DECRYPT_ONLY:
                            // 用TRP密钥解密，无需再加密
                            sql.append(String.format(
                                    "aes_decrypt(%s, '%s') AS %s",
                                    quotedFieldName,
                                    encryptConfig.getAesKey(),
                                    quotedFieldName
                            ));
                            break;
                        case ENCRYPT_ONLY:
                            // 直接用统一视图密钥加密
                            sql.append(String.format(
                                    "sm4_encrypt(%s, '%s') AS %s",
                                    quotedFieldName,
                                    encryptConfig.getSm4Key(),
                                    quotedFieldName
                            ));
                            break;
                        case DECRYPT_AND_ENCRYPT:
                            // 先用TRP密钥解密，再标准化（如果是证件号），最后用统一视图密钥加密
                            if ("idNumber".equals(fieldName)) {
                                // 证件号：解密 -> 标准化 -> 加密
                                sql.append(String.format(
                                        "sm4_encrypt(normalize_document(aes_decrypt(%s, '%s')), '%s') AS %s",
                                        quotedFieldName,
                                        encryptConfig.getAesKey(),
                                        encryptConfig.getSm4Key(),
                                        quotedFieldName
                                ));
                            } else {
                                // 其他字段：解密 -> 加密
                                sql.append(String.format(
                                        "sm4_encrypt(aes_decrypt(%s, '%s'), '%s') AS %s",
                                        quotedFieldName,
                                        encryptConfig.getAesKey(),
                                        encryptConfig.getSm4Key(),
                                        quotedFieldName
                                ));
                            }
                            break;
                        default:
                            sql.append(quotedFieldName);
                            break;
                    }
                } else {
                    sql.append(quotedFieldName);
                }
            } else {
                sql.append(quotedFieldName);
            }
        }

        sql.append(" FROM SOURCE_TABLE");
        return sql.toString();
    }

    /**
     * 加密字段配置类
     */
    public static class EncryptFieldConfig {
        /**
         * 加解密模式枚举
         */
        public enum EncryptMode {
            DECRYPT_ONLY,           // 用TRP密钥解密，无需再加密
            ENCRYPT_ONLY,           // 直接用统一视图密钥加密
            DECRYPT_AND_ENCRYPT     // 先用TRP密钥解密，再用统一视图密钥加密
        }

        private final java.util.Map<String, EncryptMode> fieldModeMap;  // 字段名到处理模式的映射
        private final String aesKey;           // AES解密密钥（32位HEX）
        private final String sm4Key;           // SM4加密密钥（Base64）

        /**
         * 构造函数
         *
         * @param fieldModeMap 字段名到处理模式的映射（如：{"bankOrderNumber": DECRYPT_ONLY, "idNumber": DECRYPT_AND_ENCRYPT}）
         * @param aesKey        AES解密密钥（32位HEX字符串，为null则使用默认密钥）
         * @param sm4Key        SM4加密密钥（Base64编码，为null则使用默认密钥）
         */
        public EncryptFieldConfig(java.util.Map<String, EncryptMode> fieldModeMap, String aesKey, String sm4Key) {
            this.fieldModeMap = fieldModeMap != null ? new java.util.HashMap<>(fieldModeMap) : new java.util.HashMap<>();
            
            // 处理AES密钥：优先使用传入的密钥，其次使用默认密钥，最后警告
            if (aesKey != null && !aesKey.isEmpty()) {
                this.aesKey = aesKey;
            } else {
                this.aesKey = ""; // 空密钥
                logger.warn("警告: AES密钥未配置，将使用空密钥！请检查Constants.TRP_AES_KEY或传入自定义密钥。");
            }
            
            // 处理SM4密钥
            if (sm4Key != null && !sm4Key.isEmpty()) {
                this.sm4Key = sm4Key;
            } else if (Constants.SM4_KEY != null && !Constants.SM4_KEY.isEmpty()) {
                this.sm4Key = Constants.SM4_KEY;
                logger.info("使用默认SM4密钥: Constants.SM4_KEY");
            } else {
                this.sm4Key = "";
                logger.warn("警告: SM4密钥未配置，将使用空密钥！请检查Constants.SM4_KEY或传入自定义密钥。");
            }
        }

        /**
         * 便捷构造函数（兼容旧版本，所有字段使用DECRYPT_AND_ENCRYPT模式）
         *
         * @param encryptFields 需要加解密的字段名数组（如：["certNumber", "mobile"]）
         * @param aesKey        AES解密密钥（32位HEX字符串，为null则使用默认密钥）
         * @param sm4Key        SM4加密密钥（Base64编码，为null则使用默认密钥）
         */
        public EncryptFieldConfig(String[] encryptFields, String aesKey, String sm4Key) {
            this.fieldModeMap = new java.util.HashMap<>();
            if (encryptFields != null) {
                for (String field : encryptFields) {
                    this.fieldModeMap.put(field, EncryptMode.DECRYPT_AND_ENCRYPT);
                }
            }
            
            // 处理AES密钥
            if (aesKey != null && !aesKey.isEmpty()) {
                this.aesKey = aesKey;
            } else {
                this.aesKey = "";
                logger.warn("警告: AES密钥未配置，将使用空密钥！请检查Constants.TRP_AES_KEY或传入自定义密钥。");
            }
            
            // 处理SM4密钥
            if (sm4Key != null && !sm4Key.isEmpty()) {
                this.sm4Key = sm4Key;
            } else if (Constants.SM4_KEY != null && !Constants.SM4_KEY.isEmpty()) {
                this.sm4Key = Constants.SM4_KEY;
                logger.info("使用默认SM4密钥: Constants.SM4_KEY");
            } else {
                this.sm4Key = "";
                logger.warn("警告: SM4密钥未配置，将使用空密钥！请检查Constants.SM4_KEY或传入自定义密钥。");
            }
        }

        /**
         * 便捷构造函数（使用默认密钥）
         */
        public EncryptFieldConfig(String[] encryptFields) {
            this(encryptFields, null, null);
        }

        /**
         * 获取字段的加解密模式
         */
        public EncryptMode getEncryptMode(String fieldName) {
            return fieldModeMap != null ? fieldModeMap.get(fieldName) : null;
        }

        /**
         * 判断字段是否需要加密（兼容旧版本）
         */
        public boolean needsEncryption(String fieldName) {
            return fieldModeMap != null && fieldModeMap.containsKey(fieldName);
        }

        /**
         * 判断是否配置了加密字段
         */
        public boolean hasEncryptFields() {
            return fieldModeMap != null && !fieldModeMap.isEmpty();
        }

        public String getAesKey() {
            return aesKey;
        }

        public String getSm4Key() {
            return sm4Key;
        }
    }

    /**
     * 创建汇率表的ID处理器
     * ID生成规则：日期签名（yyyyMMdd）+ 货币代号（currencyCode）
     * 例如：20251029USD、20251029EUR
     * 
     * @param dateSignature 日期签名（yyyyMMdd格式）
     * @param <T> 实体类型（必须是TOdsSccrmExchangeRateDetail）
     * @return ID处理函数
     */
    private static <T> java.util.function.BiConsumer<T, com.alibaba.excel.context.AnalysisContext> createExchangeRateIdProcessor(String dateSignature) {
        return new ExchangeRateIdProcessor<>(dateSignature);
    }

    /**
     * 可序列化的后处理器：先基于传入日期设置 etlDate，再复用默认ID生成规则
     */
    private static class EtlDateAndDefaultIdProcessor<T> implements java.util.function.BiConsumer<T, com.alibaba.excel.context.AnalysisContext>, java.io.Serializable {
        private static final long serialVersionUID = 1L;
        private final String dateSignature;      // yyyyMMdd
        private final String hdfsFilePath;       // 为默认ID生成器提供文件日期

        public EtlDateAndDefaultIdProcessor(String dateSignature, String hdfsFilePath) {
            this.dateSignature = dateSignature;
            this.hdfsFilePath = hdfsFilePath;
        }

        @Override
        public void accept(T data, com.alibaba.excel.context.AnalysisContext ctx) {
            if (data == null) {
                return;
            }
            try {
                String yyyy_MM_dd = java.time.LocalDate.parse(dateSignature, java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"))
                        .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                HdfsFileSource.setFieldValue(data, "etlDate", yyyy_MM_dd);
            } catch (Exception ignore) {}

            // 复用默认ID生成器（可序列化类）
            java.util.function.BiConsumer<T, com.alibaba.excel.context.AnalysisContext> idProcessor = HdfsFileSource.createDefaultIdProcessor(hdfsFilePath);
            idProcessor.accept(data, ctx);
        }
    }

    /**
     * 汇率表ID处理器（可序列化的实现类）
     * ID格式：汇率月份(yyyyMM) + 货币代号
     * 例如：202510USD、202510EUR
     */
    private static class ExchangeRateIdProcessor<T> implements java.util.function.BiConsumer<T, com.alibaba.excel.context.AnalysisContext>, java.io.Serializable {
        private static final long serialVersionUID = 1L;

        public ExchangeRateIdProcessor(String dateSignature) {
            // dateSignature参数保留用于向后兼容，但实际不使用
        }

        @Override
        public void accept(T data, com.alibaba.excel.context.AnalysisContext context) {
            if (data == null) {
                return;
            }

            try {
                // 通过反射获取exchangeMonth和currencyCode字段值
                String exchangeMonth = (String) data.getClass().getMethod("getExchangeMonth").invoke(data);
                String currencyCode = (String) data.getClass().getMethod("getCurrencyCode").invoke(data);
                
                if (exchangeMonth != null && !exchangeMonth.trim().isEmpty() 
                    && currencyCode != null && !currencyCode.trim().isEmpty()) {
                    // 生成ID：汇率月份 + 货币代号
                    String id = exchangeMonth.trim() + currencyCode.trim();
                    HdfsFileSource.setFieldValue(data, "id", id);
                    // 注意：不能在序列化类中使用外部logger，改为System.out
                    System.out.println("设置汇率表ID: " + id);
                } else {
                    System.err.println("汇率月份或货币代号为空，无法生成ID，行索引: " + context.readRowHolder().getRowIndex() 
                        + ", exchangeMonth=" + exchangeMonth + ", currencyCode=" + currencyCode);
                }
            } catch (Exception e) {
                System.err.println("设置汇率表ID失败，行索引: " + context.readRowHolder().getRowIndex() + ", 错误: " + e.getMessage());
            }
        }
    }

    /**
     * 【批量处理推荐】批量处理多个日期的数据（单个Flink Job）
     * 将多个日期的HDFS文件合并在一个Flink Job中处理，大幅提升效率
     * 
     * @param hdfsFilePathList  HDFS文件路径列表（每个路径对应一个日期）
     * @param entityClass       实体类
     * @param filePrefix        文件前缀（用于命名）
     * @param encryptConfig     加密字段配置（可选）
     * @param <T>               实体类泛型
     * @throws Exception        处理异常
     */
    public static <T> void processBatchDataFromHdfsWithSQL(List<HdfsFileInfo> hdfsFilePathList,
                                                            Class<T> entityClass,
                                                            String filePrefix,
                                                            EncryptFieldConfig encryptConfig) throws Exception {
        if (hdfsFilePathList == null || hdfsFilePathList.isEmpty()) {
            logger.warn("HDFS文件路径列表为空，无数据需要处理");
            return;
        }

        logger.info("开始批量处理 {} 个日期的数据", hdfsFilePathList.size());

        // 按月分批处理：将文件按月份分组，每个月作为一个批次提交
        Map<String, List<HdfsFileInfo>> monthlyBatches = groupByMonth(hdfsFilePathList);
        
        if (monthlyBatches.size() > 1) {
            logger.info("检测到跨月数据，将按月份分批处理，共 {} 个月", monthlyBatches.size());
            int batchIndex = 1;
            int totalBatches = monthlyBatches.size();
            
            // 按月份排序（确保按时间顺序处理）
            List<String> sortedMonths = new ArrayList<>(monthlyBatches.keySet());
            sortedMonths.sort(String::compareTo);
            
            for (String monthKey : sortedMonths) {
                List<HdfsFileInfo> monthlyBatch = monthlyBatches.get(monthKey);
                logger.info("处理第 {}/{} 批（{}），包含 {} 个文件", 
                    batchIndex, totalBatches, monthKey, monthlyBatch.size());
                processBatchDataFromHdfsWithSQLInternal(monthlyBatch, entityClass, filePrefix, encryptConfig, batchIndex, totalBatches);
                batchIndex++;
            }
            logger.info("所有月份批次处理完成，共处理 {} 个文件，跨越 {} 个月", 
                hdfsFilePathList.size(), totalBatches);
            return;
        }

        // 单月数据，直接处理
        logger.info("单月数据，直接处理");
        processBatchDataFromHdfsWithSQLInternal(hdfsFilePathList, entityClass, filePrefix, encryptConfig, 1, 1);
    }

    /**
     * 内部批量处理方法（实际执行Flink作业）
     */
    private static <T> void processBatchDataFromHdfsWithSQLInternal(List<HdfsFileInfo> hdfsFilePathList,
                                                                    Class<T> entityClass,
                                                                    String filePrefix,
                                                                    EncryptFieldConfig encryptConfig,
                                                                    int batchIndex,
                                                                    int totalBatches) throws Exception {
        logger.info("开始处理批次 {}/{}，包含 {} 个文件", batchIndex, totalBatches, hdfsFilePathList.size());

        // 1. 创建Flink执行环境
        // 使用getExecutionEnvironment()会自动检测环境：
        // - 如果检测到Yarn环境，会自动提交到Yarn集群
        // - 如果是本地环境，会创建本地执行环境
        // 这样任务提交到Yarn后，本地程序就可以退出了，不需要等待任务完成
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 优化：根据文件数量和数据量动态设置并行度
        int fileCount = hdfsFilePathList.size();
        int optimalParallelism = Math.min(
            Math.max(2, Math.min(fileCount, 50)), 
            Runtime.getRuntime().availableProcessors()
        );
        env.setParallelism(optimalParallelism);
        logger.info("设置Flink并行度为: {}", optimalParallelism);
        logger.info("Flink环境类型: {} (如果检测到Yarn环境，任务将自动提交到Yarn集群)", 
            env.getClass().getSimpleName());
        
        // 生成批量任务的唯一标识
        String batchId = filePrefix + "-batch-" + System.currentTimeMillis();
        if (totalBatches > 1) {
            batchId += "-part" + batchIndex + "of" + totalBatches;
        }
        
        // 优化：批量处理历史数据时，禁用checkpoint以减少开销
        // 历史数据不需要容错恢复，checkpoint会显著降低性能
        // CheckpointUtils.setCheckpoint(env, "TrpHdfsBatch-" + batchId);

        // 2. 创建Table环境
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 3. 注册UDF函数
        if (encryptConfig != null && encryptConfig.hasEncryptFields()) {
            tEnv.createTemporarySystemFunction("aes_decrypt", TrpAESDecryptor.class);
            tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
            tEnv.createTemporarySystemFunction("normalize_document", DocumentNumberNormalizeUDF.class);
            logger.info("已注册AES解密、SM4加密和证件号标准化UDF函数");
        }

        // 4. 合并所有日期的数据流
        TypeInformation<T> typeInfo = TypeExtractor.createTypeInfo(entityClass);
        DataStream<T> mergedDataStream = null;

        for (HdfsFileInfo fileInfo : hdfsFilePathList) {
            String hdfsFilePath = fileInfo.getHdfsFilePath();
            String dateSignature = fileInfo.getDateSignature();
            
            if (hdfsFilePath == null || hdfsFilePath.trim().isEmpty()) {
                logger.warn("跳过无效的HDFS文件路径: 日期={}, 路径为空", dateSignature);
                continue;
            }
            
            logger.info("添加数据源: 日期={}, 路径={}", dateSignature, hdfsFilePath);

            DataStream<T> singleDateStream;

            // 根据实体类配置不同的文件解析参数
            if (entityClass == TOdsYxwSaleReport.class) {
                singleDateStream = env.addSource(
                        new HdfsFileSource<>(hdfsFilePath, entityClass, '↑', StandardCharsets.UTF_8, 1,
                                new EtlDateAndDefaultIdProcessor<>(dateSignature, hdfsFilePath))
                ).returns(typeInfo).name("HDFS-Source-" + dateSignature);
            } else if (entityClass == TOdsSccrmExchangeRateDetail.class) {
                singleDateStream = env.addSource(
                        new HdfsFileSource<>(hdfsFilePath, entityClass, '|', Charset.forName("GBK"), 1,
                                createExchangeRateIdProcessor(dateSignature))
                ).returns(typeInfo).name("HDFS-Source-" + dateSignature);
            } else {
                singleDateStream = env.addSource(
                        new HdfsFileSource<>(hdfsFilePath, entityClass, ',', Charset.forName("GBK"), 2,
                                new EtlDateAndDefaultIdProcessor<>(dateSignature, hdfsFilePath))
                ).returns(typeInfo).name("HDFS-Source-" + dateSignature);
            }

            // 合并数据流
            if (mergedDataStream == null) {
                mergedDataStream = singleDateStream;
            } else {
                mergedDataStream = mergedDataStream.union(singleDateStream);
            }
        }

        if (mergedDataStream == null) {
            logger.error("合并后的数据流为空");
            throw new RuntimeException("合并后的数据流为空，无法继续处理");
        }

        logger.info("数据流合并完成，共 {} 个数据源", hdfsFilePathList.size());

        // 5. 将合并后的DataStream转换为Table
        Table sourceTable = tEnv.fromDataStream(mergedDataStream);
        tEnv.createTemporaryView("SOURCE_TABLE", sourceTable);

        // 6. 构建SQL查询语句
        String selectSql = buildSelectSql(entityClass, encryptConfig);
        logger.info("执行SQL查询: {}", selectSql);

        // 7. 执行SQL查询
        Table resultTable = tEnv.sqlQuery(selectSql);

        // 8. 转换为ChangelogStream
        DataStream<org.apache.flink.types.Row> rowStream = tEnv.toChangelogStream(resultTable);

        // 9. Row转实体，注意：批量模式下需从数据中提取实际的etlDate
        DataStream<T> processedStream = rowStream
                .map(row -> rowToEntityBatch(row, entityClass))
                .returns(typeInfo)
                .name("Row-To-Entity-Batch-" + filePrefix);

        // 10. 创建Doris Sink并写入数据
        String tableName = getTableNameByEntity(entityClass).toUpperCase();
        DorisSink<T> dorisSink = FlinkDorisUtils.creatDorisODSSinkForFTP(tableName);
        processedStream.sinkTo(dorisSink).name("Doris-Sink-" + tableName);

        // 11. 执行Flink作业
        // 使用getExecutionEnvironment()时，如果检测到Yarn环境，会自动提交到Yarn集群
        // 使用executeAsync()可以提交任务后立即返回，不等待任务完成
        String jobName = "TRP-HDFS-Batch-" + batchId;
        logger.info("提交Flink作业到集群: {}", jobName);
        
        try {
            // 使用异步执行：提交任务到Yarn后立即返回，不等待任务完成
            // 这样本地程序可以继续处理下一批次，任务在Yarn集群上独立运行
            org.apache.flink.core.execution.JobClient jobClient = env.executeAsync(jobName);
            logger.info("批次 {}/{} Flink作业已成功提交到集群，JobClient: {}", 
                batchIndex, totalBatches, jobClient.getJobID());
            logger.info("任务已在Yarn集群上运行，本地程序继续处理下一批次");
        } catch (Exception e) {
            logger.error("批次 {}/{} Flink作业提交失败: {}", batchIndex, totalBatches, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * 批量模式下Row转实体（从ID字段提取etlDate）
     * 由于批量模式合并了多个日期的数据，需要从每行数据的ID字段中提取实际的etlDate
     */
    private static <T> T rowToEntityBatch(org.apache.flink.types.Row row, Class<T> entityClass) {
        try {
            T entity = entityClass.newInstance();
            
            // 获取所有字段
            java.lang.reflect.Field[] fields = entityClass.getDeclaredFields();
            int fieldIndex = 0;
            String extractedEtlDate = null;
            
            for (java.lang.reflect.Field field : fields) {
                // 跳过静态字段
                if (java.lang.reflect.Modifier.isStatic(field.getModifiers()) || 
                    "serialVersionUID".equals(field.getName())) {
                    continue;
                }
                
                field.setAccessible(true);
                Object value = row.getField(fieldIndex);
                
                if (value != null) {
                    field.set(entity, value);
                    
                    // 从ID字段提取etlDate（ID格式：yyyyMMdd + 序号）
                    if ("id".equals(field.getName()) && value instanceof String) {
                        String idValue = (String) value;
                        if (idValue.length() >= 8) {
                            String datePart = idValue.substring(0, 8);
                            // 验证是否为有效日期格式
                            if (datePart.matches("\\d{8}")) {
                                extractedEtlDate = datePart;
                            }
                        }
                    }
                }
                
                fieldIndex++;
            }
            
            // 优先从行数据中的 etlDate 字段获取（来源于文件日期与传入日期匹配）
            try {
                java.lang.reflect.Field etlDateField = entityClass.getDeclaredField("etlDate");
                etlDateField.setAccessible(true);
                Object etlDateValue = etlDateField.get(entity);
                if (etlDateValue instanceof String) {
                    String dateStr = (String) etlDateValue; // 期望格式 yyyy-MM-dd
                    if (dateStr != null && dateStr.length() >= 10) {
                        String compact = dateStr.replace("-", "");
                        if (compact.matches("\\d{8}")) {
                            setEtlFields(entity, compact);
                            return entity;
                        }
                    }
                }
            } catch (NoSuchFieldException ignore) {
                // 实体无 etlDate 字段则略过
            }

            if (extractedEtlDate != null) {
                setEtlFields(entity, extractedEtlDate);
            } else {
                Object idObj = row.getField(0);
                if (idObj != null) {
                    String idStr = String.valueOf(idObj);
                    if (idStr.length() >= 8 && idStr.substring(0, 8).matches("\\d{8}")) {
                        setEtlFields(entity, idStr.substring(0, 8));
                        return entity;
                    }
                }
                String currentDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                setEtlFields(entity, currentDate);
            }
            
            return entity;
        } catch (Exception e) {
            logger.error("Row转实体失败: {}", e.getMessage(), e);
            throw new RuntimeException("Row转实体失败", e);
        }
    }

    /**
     * HDFS文件信息封装类（用于批量处理）
     */
    public static class HdfsFileInfo implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        
        private final String hdfsFilePath;
        private final String dateSignature;

        public HdfsFileInfo(String hdfsFilePath, String dateSignature) {
            this.hdfsFilePath = hdfsFilePath;
            this.dateSignature = dateSignature;
        }

        public String getHdfsFilePath() {
            return hdfsFilePath;
        }

        public String getDateSignature() {
            return dateSignature;
        }
    }

    /**
     * 根据实体类获取对应的表名
     */
    private static String getTableNameByEntity(Class<?> entityClass) {
        return ENTITY_TABLE_MAP.getOrDefault(entityClass, "unknown_table");
    }

    /**
     * 按月份分组文件列表
     * @param hdfsFilePathList 文件列表
     * @return 按月份分组的Map，key为"yyyyMM"格式的月份，value为该月的文件列表
     */
    private static Map<String, List<HdfsFileInfo>> groupByMonth(List<HdfsFileInfo> hdfsFilePathList) {
        Map<String, List<HdfsFileInfo>> monthlyMap = new HashMap<>();
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        
        for (HdfsFileInfo fileInfo : hdfsFilePathList) {
            String dateSignature = fileInfo.getDateSignature();
            if (dateSignature == null || dateSignature.length() < 8) {
                logger.warn("文件日期格式不正确，跳过: {}", fileInfo.getHdfsFilePath());
                continue;
            }
            
            try {
                // 提取月份（yyyyMM格式）
                String monthKey = dateSignature.substring(0, 6);
                
                // 验证日期格式
                LocalDate.parse(dateSignature, dateFormatter);
                
                monthlyMap.computeIfAbsent(monthKey, k -> new ArrayList<>()).add(fileInfo);
            } catch (Exception e) {
                logger.warn("解析文件日期失败，跳过: {}, 日期: {}", fileInfo.getHdfsFilePath(), dateSignature, e);
            }
        }
        
        logger.info("按月份分组完成，共 {} 个月: {}", monthlyMap.size(), monthlyMap.keySet());
        return monthlyMap;
    }
}
