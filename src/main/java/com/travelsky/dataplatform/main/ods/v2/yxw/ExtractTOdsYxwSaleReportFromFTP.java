package com.travelsky.dataplatform.main.ods.v2.yxw;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.yxw.TOdsYxwSaleReport;
import com.travelsky.trp.usercenter.data.analysis.utils.FTPConfigUtils;
import com.travelsky.trp.usercenter.data.analysis.utils.TrpFlinkTransferUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * 国际客票销售数据报表转储任务（YXW）
 * 完整流程：SFTP → HDFS → Flink → Doris
 * 文件格式：YXW_SALE_DATA_YYYYMMDD.CSV
 * 数据源：国际销售FTP服务器 /financial_sales 目录
 * 目标表：t_ods_yxw_sale_report
 * 
 * 使用方式：
 * java ExtractTOdsYxwSaleReportFromFTP 2025-01-15
 */
public class ExtractTOdsYxwSaleReportFromFTP {

    private static final Logger logger = LoggerFactory.getLogger(ExtractTOdsYxwSaleReportFromFTP.class);

    public static void main(String[] args) {
        try {
            logger.info("开始执行国际客票销售数据报表转储（YXW）...");
            
            // 1. 参数校验
            if (args.length < 1) {
                logger.error("缺少ETL日期参数，格式：yyyy-MM-dd");
                System.exit(1);
            }
            
            // 2. 解析日期参数（输入格式：yyyy-MM-dd，文件查找格式：yyyyMMdd）
            String etlDate = args[0];
            String fileDate = convertToFileDateFormat(etlDate);
            logger.info("ETL日期: {}", etlDate);
            logger.info("文件日期: {}", fileDate);

            // 3. 配置SFTP连接信息（国际销售数据专用FTP）
            FTPConfigUtils.SftpConfig sftpConfig = new FTPConfigUtils.SftpConfig(
                    Constants.INTL_SALES_FTP_HOST,
                    Integer.parseInt(Constants.INTL_SALES_FTP_PORT),
                    Constants.INTL_SALES_FTP_USERNAME,
                    Constants.INTL_SALES_FTP_PWD,
                    Constants.INTL_SALES_FTP_PATH  // YXW国际销售数据存放目录
            );

            // 4. 从SFTP下载文件到HDFS
            logger.info("======================================");
            logger.info("步骤1: 从SFTP下载文件到HDFS...");
            String hdfsFilePath = TrpFlinkTransferUtils.downloadFromSftpToHdfs(
                    sftpConfig,
                    "YXW_SALE_DATA",
                    fileDate
            );

            if (hdfsFilePath == null) {
                logger.error("从SFTP下载文件到HDFS失败，FTP文件未找到，跳过本次处理");
                return;
            }

            logger.info("文件已上传到HDFS: {}", hdfsFilePath);

            // 5. 使用Flink处理数据
            logger.info("======================================");
            logger.info("步骤2: 使用Flink处理数据...");
            
            TrpFlinkTransferUtils.processDataFromHdfsWithSQL(
                    hdfsFilePath,
                    TOdsYxwSaleReport.class,
                    fileDate,
                    "YXW_SALE_DATA",
                    null  // 不需要加解密
            );

            logger.info("======================================");
            logger.info("国际客票销售数据报表转储完成（YXW）！");

        } catch (Exception e) {
            logger.error("国际客票销售数据报表转储异常: {}", e.getMessage(), e);
        }
    }

    /**
     * 将日期从 yyyy-MM-dd 格式转换为 yyyyMMdd 格式（用于文件查找）
     * @param dateStr 日期字符串（格式：yyyy-MM-dd）
     * @return 日期字符串（格式：yyyyMMdd）
     */
    private static String convertToFileDateFormat(String dateStr) {
        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        DateTimeFormatter fileFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate date = LocalDate.parse(dateStr, inputFormatter);
        return date.format(fileFormatter);
    }
}
