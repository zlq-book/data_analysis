package com.travelsky.trp.usercenter.data.analysis.utils;

import com.alibaba.excel.context.AnalysisContext;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.KerberosAuthUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * HDFS文件数据源 - Flink SourceFunction
 * 从HDFS读取文件并解析为实体对象流
 * 支持自动识别文件格式（Excel/CSV）并选择合适的解析器
 * 
 * 参数说明：
 * - delimiter: 仅在CSV文件中生效
 * - charset: CSV和Excel均可使用（Excel通常自动识别编码）
 * - headRowNumber: CSV和Excel均可使用，指定表头行数
 * - postProcessor: CSV和Excel均可使用，用于自定义后处理逻辑
 *
 * @param <T> 实体类类型
 */
public class HdfsFileSource<T> implements SourceFunction<T> {

    private static final Logger logger = LoggerFactory.getLogger(HdfsFileSource.class);
    
    private final String hdfsFilePath;
    private final Class<T> entityClass;
    private final char delimiter;              // CSV分隔符（仅CSV生效）
    private final String charsetName;          // 字符编码名称（可序列化，CSV必需，Excel可选）
    private final Integer headRowNumber;        // 表头行数（Excel和CSV均生效）
    private final BiConsumer<T, AnalysisContext> postProcessor;  // 后处理函数
    private volatile boolean isRunning = true;

    /**
     * 完整构造函数（带后处理函数）
     * 适用于需要完全控制所有参数的场景
     *
     * @param hdfsFilePath HDFS文件路径（完整路径，如：hdfs://cmss/data/trp/sc_b2c_report_sale_20251010.csv）
     * @param entityClass  实体类
     * @param delimiter    CSV分隔符（仅对CSV文件生效，Excel文件忽略此参数）
     * @param charset      字符编码（CSV必需，Excel可选）
     * @param headRowNumber 表头行号（Excel和CSV均生效，1表示第一行为表头）
     * @param postProcessor 后处理函数，用于处理每条记录（如ID生成），为null时使用默认ID处理器
     */
    public HdfsFileSource(String hdfsFilePath,
                          Class<T> entityClass, 
                          char delimiter, 
                          Charset charset, 
                          Integer headRowNumber,
                          BiConsumer<T, AnalysisContext> postProcessor) {
        if (hdfsFilePath == null || hdfsFilePath.trim().isEmpty()) {
            throw new IllegalArgumentException("HDFS文件路径不能为空");
        }
        if (entityClass == null) {
            throw new IllegalArgumentException("实体类不能为空");
        }
        if (charset == null) {
            throw new IllegalArgumentException("字符编码不能为空");
        }
        if (headRowNumber == null || headRowNumber < 1) {
            throw new IllegalArgumentException("表头行号必须大于0");
        }
        this.hdfsFilePath = hdfsFilePath;
        this.entityClass = entityClass;
        this.delimiter = delimiter;
        this.charsetName = charset.name();
        this.headRowNumber = headRowNumber;
        this.postProcessor = (postProcessor != null) ? postProcessor : createDefaultIdProcessor(hdfsFilePath);
    }

    /**
     * 构造函数（不带后处理函数）
     * 适用于不需要自定义ID生成等后处理逻辑的场景
     *
     * @param hdfsFilePath HDFS文件路径
     * @param entityClass  实体类
     * @param delimiter    CSV分隔符（仅对CSV文件生效）
     * @param charset      字符编码
     * @param headRowNumber 表头行号
     */
    public HdfsFileSource(String hdfsFilePath,
                          Class<T> entityClass, 
                          char delimiter, 
                          Charset charset, 
                          Integer headRowNumber) {
        this(hdfsFilePath, entityClass, delimiter, charset, headRowNumber, null);
    }

    /**
     * 简化构造函数（使用默认参数：逗号分隔符、UTF-8编码、第1行为表头）
     * 适用于标准CSV/Excel文件
     *
     * @param hdfsFilePath HDFS文件路径
     * @param entityClass  实体类
     */
    public HdfsFileSource(String hdfsFilePath,
                          Class<T> entityClass) {
        this(hdfsFilePath, entityClass, ',', StandardCharsets.UTF_8, 1, null);
    }

    /**
     * 简化构造函数（带后处理函数，使用默认参数）
     * 适用于需要自定义ID生成但其他参数使用默认值的场景
     *
     * @param hdfsFilePath HDFS文件路径
     * @param entityClass  实体类
     * @param postProcessor 后处理函数（如自定义ID生成逻辑）
     */
    public HdfsFileSource(String hdfsFilePath,
                          Class<T> entityClass,
                          BiConsumer<T, AnalysisContext> postProcessor) {
        this(hdfsFilePath, entityClass, ',', StandardCharsets.UTF_8, 1, postProcessor);
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        FileSystem fs = null;
        InputStream inputStream = null;

        try {
            if (hdfsFilePath == null || hdfsFilePath.trim().isEmpty()) {
                logger.error("HDFS文件路径为空，无法创建Path对象");
                throw new IllegalArgumentException("HDFS文件路径不能为空");
            }

            Configuration conf = buildHdfsConfiguration();
            fs = FileSystem.newInstance(conf);

            Path filePath = new Path(hdfsFilePath);

            // 检查文件是否存在
            if (!fs.exists(filePath)) {
                logger.warn("HDFS文件不存在: {}", hdfsFilePath);
                return;
            }

            logger.info("开始从HDFS读取文件: {}", hdfsFilePath);

            // 打开文件流，使用BufferedInputStream包装以支持mark/reset（用于编码检测）
            inputStream = new java.io.BufferedInputStream(fs.open(filePath));

            // 根据文件后缀自动选择解析方式
            String lowerFileName = hdfsFilePath.toLowerCase();
            List<T> dataList = new ArrayList<>();

            if (lowerFileName.endsWith(".csv")) {
                // CSV文件解析（自动检测编码，使用delimiter和charset参数）
                logger.info("检测到CSV文件，使用CSV解析器 [分隔符='{}', 原配置编码={}, 表头行数={}]", 
                        delimiter, charsetName, headRowNumber);
                dataList = parseCsvFile(inputStream);
            } else if (lowerFileName.endsWith(".xlsx") || lowerFileName.endsWith(".xls")) {
                // Excel文件解析（使用headRowNumber参数，delimiter不适用）
                logger.info("检测到Excel文件，使用Excel解析器 [表头行数={}]", headRowNumber);
                dataList = parseExcelFile(inputStream);
            } else {
                logger.warn("未知文件类型，默认使用Excel解析器: {}", hdfsFilePath);
                dataList = parseExcelFile(inputStream);
            }

            logger.info("从HDFS文件 {} 读取到 {} 条记录", hdfsFilePath, dataList.size());

            // 将数据发送到Flink数据流
            synchronized (ctx.getCheckpointLock()) {
                for (T data : dataList) {
                    if (isRunning && data != null) {
                        ctx.collect(data);
                    }
                }
            }

            logger.info("HDFS数据源处理完成");

        } catch (Exception e) {
            logger.error("HDFS数据源处理异常: {}", e.getMessage(), e);
            throw e;
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception e) {
                    logger.warn("关闭HDFS输入流失败: {}", e.getMessage());
                }
            }
            if (fs != null) {
                try {
                    fs.close();
                    logger.info("HDFS连接已关闭");
                } catch (Exception e) {
                    logger.warn("关闭HDFS连接失败: {}", e.getMessage());
                }
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    /**
     * 构建HDFS配置（包含Kerberos认证）
     */
    private Configuration buildHdfsConfiguration() {
        System.setProperty("java.security.krb5.conf", Constants.KRB5_CONF_PATH);
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://cmss");
        conf.set("dfs.nameservices", "cmss");
        conf.set("dfs.ha.namenodes.cmss", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.cmss.nn1", "emr-master-001.novalocal:8020");
        conf.set("dfs.namenode.rpc-address.cmss.nn2", "emr-master-ha-001.novalocal:8020");
        conf.set("dfs.client.failover.proxy.provider.cmss",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        

//        try {
//            KerberosAuthUtils.initKerberosAuth(
//                    Constants.KRB5_CONF_PATH,
//                    Constants.SYS_UCV_TEST_FOR_MAPREDUCE_PRINCIPAL,
//                    Constants.SYS_UCV_TEST_FOR_MAPREDUCE_KEYTAB_PATH
//            );
//            logger.info("Kerberos认证成功（使用KerberosAuthUtils工具类）: {}", Constants.SYS_UCV_TEST_FOR_MAPREDUCE_PRINCIPAL);
//        } catch (Exception e) {
//            logger.error("Kerberos认证失败: {}", e.getMessage(), e);
//            throw new RuntimeException("Kerberos认证失败", e);
//        }

        return conf;
    }

    /**
     * 解析CSV文件（内部方法）
     * 自动检测文件编码（GBK或UTF-8），然后选择合适的解析方法
     * 注意：inputStream应该是BufferedInputStream，以支持mark/reset操作
     */
    private List<T> parseCsvFile(InputStream inputStream) {
        Charset detectedCharset;
        
        try {
            java.io.BufferedInputStream bufferedStream;
            if (inputStream instanceof java.io.BufferedInputStream) {
                bufferedStream = (java.io.BufferedInputStream) inputStream;
            } else {
                bufferedStream = new java.io.BufferedInputStream(inputStream);
            }
            
            detectedCharset = EasyExcelUtils.detectCsvEncoding(bufferedStream);
            logger.info("CSV文件编码检测完成: {} (原配置: {})", detectedCharset.name(), charsetName);
            
            if (detectedCharset.equals(StandardCharsets.UTF_8)) {
                return EasyExcelUtils.readCsvFromStreamUTF8(
                        bufferedStream, entityClass, delimiter, headRowNumber, postProcessor);
            } else if (detectedCharset.name().equalsIgnoreCase("GBK")) {
                return EasyExcelUtils.readCsvFromStreamGBK(
                        bufferedStream, entityClass, delimiter, headRowNumber, postProcessor);
            } else {
                logger.warn("检测到不支持的编码格式: {}，尝试使用UTF-8", detectedCharset.name());
                return EasyExcelUtils.readCsvFromStreamUTF8(
                        bufferedStream, entityClass, delimiter, headRowNumber, postProcessor);
            }
        } catch (Exception e) {
            logger.error("编码检测或解析失败，使用原配置编码 {}: {}", charsetName, e.getMessage(), e);
            try {
                Charset charset = Charset.forName(charsetName);
                if (charset.equals(StandardCharsets.UTF_8)) {
                    return EasyExcelUtils.readCsvFromStreamUTF8(
                            inputStream, entityClass, delimiter, headRowNumber, postProcessor);
                } else if (charsetName.equalsIgnoreCase("GBK")) {
                    return EasyExcelUtils.readCsvFromStreamGBK(
                            inputStream, entityClass, delimiter, headRowNumber, postProcessor);
                } else {
                    return EasyExcelUtils.readCsvFromStreamUTF8(
                            inputStream, entityClass, delimiter, headRowNumber, postProcessor);
                }
            } catch (Exception ex) {
                logger.error("使用原配置编码解析也失败: {}", ex.getMessage(), ex);
                throw new RuntimeException("CSV文件解析失败", ex);
            }
        }
    }

    /**
     * 解析Excel文件（内部方法）
     * Excel文件不需要delimiter参数，但支持headRowNumber和postProcessor
     */
    private List<T> parseExcelFile(InputStream inputStream) {
        return EasyExcelUtils.readExcelWithHeadRowFromStream(
                inputStream, entityClass, headRowNumber, postProcessor);
    }

    /**
     * 从文件名提取日期（yyyyMMdd格式）
     * 例如：/data/trp/sc_b2c_report_sale_20251010.csv -> 20251010
     * 
     * @param filePath 文件路径
     * @return 提取的日期字符串（8位），如果提取失败则返回null
     */
    public static String extractDateFromFileName(String filePath) {
        try {
            // 提取文件名（去除路径）
            String fileName = filePath;
            if (filePath.contains("/")) {
                fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
            } else if (filePath.contains("\\")) {
                fileName = filePath.substring(filePath.lastIndexOf("\\") + 1);
            }

            // 去除扩展名
            int dotIndex = fileName.lastIndexOf(".");
            if (dotIndex > 0) {
                fileName = fileName.substring(0, dotIndex);
            }

            // 使用正则提取8位日期（yyyyMMdd）
            java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("(\\d{8})");
            java.util.regex.Matcher matcher = pattern.matcher(fileName);

            if (matcher.find()) {
                String extractedDate = matcher.group(1);
                return extractedDate;
            } else {
                return null;
            }
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 使用反射设置字段值
     * 
     * @param obj 目标对象
     * @param fieldName 字段名
     * @param value 字段值
     */
    public static void setFieldValue(Object obj, String fieldName, Object value) {
        try {
            java.lang.reflect.Field field = obj.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(obj, value);
        } catch (Exception e) {
            // 忽略设置失败的情况
        }
    }

    /**
     * 创建默认的ID处理器（文件名日期 + 序号）
     * 
     * @param filePath 文件路径
     * @param <T> 实体类型
     * @return ID处理函数
     */
    public static <T> BiConsumer<T, AnalysisContext> createDefaultIdProcessor(String filePath) {
        String fileDate = extractDateFromFileName(filePath);
        if (fileDate == null) {
            fileDate = "00000000"; // 默认日期
        }
        return new DefaultIdProcessor<>(fileDate);
    }

    /**
     * 默认ID处理器（可序列化的实现类）
     * ID格式：文件名日期(yyyyMMdd) + 序号（10位补零）
     * 例如：202510290000000001、20251029000000005
     */
    private static class DefaultIdProcessor<T> implements BiConsumer<T, AnalysisContext>, java.io.Serializable {
        private static final long serialVersionUID = 1L;
        private final String fileDate;

        public DefaultIdProcessor(String fileDate) {
            this.fileDate = fileDate;
        }

        @Override
        public void accept(T data, AnalysisContext context) {
            if (data == null) {
                return;
            }

            try {
                String id;
                Long oid = (Long) data.getClass().getMethod("getId").invoke(data);
                int rowIndex = context.readRowHolder().getRowIndex();
                
                if (oid == null) {
                    // 因为有些报表是没有序号的，需要我们自动生成
                    // 生成ID：文件名日期 + 序号（序号补零10位）
                    // rowIndex 是从0开始的，第一行数据是0，所以需要+1
                    String sequence = String.format("%010d", rowIndex + 1);
                    id = fileDate + sequence;
                } else {
                    id = fileDate + oid;
                }

                // 使用反射设置ID字段
                setFieldValue(data, "id", Long.parseLong(id));
            } catch (Exception e) {
                // 忽略处理失败的情况
            }
        }
    }
}
