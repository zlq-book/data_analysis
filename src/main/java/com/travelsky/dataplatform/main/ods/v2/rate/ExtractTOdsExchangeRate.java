package com.travelsky.dataplatform.main.ods.v2.rate;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.rate.TOdsSccrmExchangeRateDetail;
import com.travelsky.trp.usercenter.data.analysis.utils.FTPConfigUtils;
import com.travelsky.trp.usercenter.data.analysis.utils.TrpFlinkTransferUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class ExtractTOdsExchangeRate {

    private static final Logger logger = LoggerFactory.getLogger(ExtractTOdsExchangeRate.class);

    public static void main(String[] args) {
        try {
            logger.info("开始执行汇率表数据转储（CSV→Doris）...");

            // --------------------------
            // 1. 参数校验
            // --------------------------
            if (args.length < 1) {
                logger.error("缺少ETL日期参数，格式：yyyy-MM-dd");
                System.exit(1);
            }
            String etlDate = args[0];
            String fileDate = convertToFileDateFormat(etlDate);
            logger.info("ETL日期: {}", etlDate);
            logger.info("文件日期: {}", fileDate);


            // 2. SFTP配置，创建一个SFTP连接配置对象，用于后续从SFTP服务器下载文件
            FTPConfigUtils.SftpConfig sftpConfig = new FTPConfigUtils.SftpConfig(
                    Constants.EXCHANGE_RATE_FTP_HOST,
                    Integer.parseInt(Constants.EXCHANGE_RATE_FTP_PORT),
                    Constants.EXCHANGE_RATE_FTP_USERNAME,
                    Constants.EXCHANGE_RATE_FTP_PWD,
                    Constants.EXCHANGE_RATE_FTP_DOWNLOAD_PATH
            );


            // --------------------------
            // 3. 步骤1：从SFTP下载汇率CSV到HDFS
            // --------------------------
            logger.info("步骤1: 从SFTP下载文件到HDFS...");
            String hdfsFilePath = TrpFlinkTransferUtils.downloadFromSftpToHdfs(
                    sftpConfig,
                    "EXRATE",
                    fileDate
            );
//            String hdfsFilePath = Constants.RATE_FTP_HDFS;
            if (hdfsFilePath == null) {
                logger.error("从SFTP下载汇率CSV到HDFS失败，FTP文件未找到，跳过本次处理");
                return;
            }
            logger.info("汇率CSV已上传到HDFS: {}", hdfsFilePath);

            // --------------------------
            // 4. 步骤2：配置加解密（汇率数据无敏感字段，跳过加解密）
            // --------------------------

            // --------------------------
            // 5. 步骤3：Flink SQL导入（适配汇率表结构）
            // --------------------------
            logger.info("步骤3: 使用Flink SQL导入数据到Doris...");

//             使用Flink SQL导入数据到Doris...
            TrpFlinkTransferUtils.processDataFromHdfsWithSQL(
                    hdfsFilePath,
                    TOdsSccrmExchangeRateDetail.class,
                    fileDate,
                    "T_ODS_EXCHANGE_RATE", // 目标文件前缀
                    null           // 无敏感字段，传null
            );

            logger.info("汇率表数据转储完成（CSV→HDFS→Doris）！");

        } catch (Exception e) {
            logger.error("汇率表数据转储异常: {}", e.getMessage(), e);
        }
    }

    private static String convertToFileDateFormat(String dateStr) {
        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        DateTimeFormatter fileFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate date = LocalDate.parse(dateStr, inputFormatter);
        return date.format(fileFormatter);
    }
}
