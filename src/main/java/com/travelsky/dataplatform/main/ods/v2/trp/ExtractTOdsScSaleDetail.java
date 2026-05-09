package com.travelsky.dataplatform.main.ods.v2.trp;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.trp.TOdsScSaleDetail;
import com.travelsky.trp.usercenter.data.analysis.utils.FTPConfigUtils;
import com.travelsky.trp.usercenter.data.analysis.utils.TrpFlinkTransferUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * SC销售报表明细数据转储脚本（SQL模式 - 推荐）
 * 使用 StreamTableEnvironment + SQL + UDF 自动化处理加解密
 * 从SFTP下载sc_b2c_report_sale_YYYYMMDD.xlsx文件并导入到ODS数据库
 * 注意：TRP系统使用的是SFTP协议，不是FTP协议
 */
public class ExtractTOdsScSaleDetail {

    private static final Logger logger = LoggerFactory.getLogger(ExtractTOdsScSaleDetail.class);

    public static void main(String[] args) {
        try {
            logger.info("开始执行SC销售报表明细数据转储（SQL模式，经HDFS）...");
            if (args.length < 1) {
                logger.error("缺少ETL日期参数，格式：yyyy-MM-dd");
                System.exit(1);
            }
            String etlDate = args[0];
            String fileDate = convertToFileDateFormat(etlDate);
            logger.info("ETL日期: {}", etlDate);
            logger.info("文件日期: {}", fileDate);

            // SFTP配置信息
            FTPConfigUtils.SftpConfig sftpConfig = new FTPConfigUtils.SftpConfig(
                    Constants.TRP_FTP_HOST,
                    Integer.parseInt(Constants.TRP_FTP_PORT),
                    Constants.TRP_FTP_USERNAME,
                    Constants.TRP_FTP_PWD,
                    Constants.TRP_FTP_DOWNLOAD_PATH
            );

            // 步骤1：从SFTP下载文件并上传到HDFS
            logger.info("步骤1: 从SFTP下载文件到HDFS...");
            String hdfsFilePath = TrpFlinkTransferUtils.downloadFromSftpToHdfs(
                    sftpConfig,
                    "sc_b2c_report_sale",
                    fileDate
            );

            if (hdfsFilePath == null) {
                logger.error("从SFTP下载文件到HDFS失败，FTP文件未找到，跳过本次处理");
                return;
            }

            logger.info("文件已上传到HDFS: {}", hdfsFilePath);

            // 步骤2：配置需要加解密的敏感字段
            // 根据规范配置加解密模式：
            // - 银行订单号：用TRP密钥解密，无需再加密
            // - 订票用户名、订票人手机号、订票人邮箱、常旅客卡号：直接用统一视图密钥加密
            // - 证件号码、联系人手机号：先用TRP密钥解密，再用统一视图密钥加密
            Map<String, TrpFlinkTransferUtils.EncryptFieldConfig.EncryptMode> fieldModeMap = new HashMap<>();
            fieldModeMap.put("bankOrderNumber", TrpFlinkTransferUtils.EncryptFieldConfig.EncryptMode.DECRYPT_ONLY);
            fieldModeMap.put("bookingUsername", TrpFlinkTransferUtils.EncryptFieldConfig.EncryptMode.ENCRYPT_ONLY);
            fieldModeMap.put("bookingUserPhone", TrpFlinkTransferUtils.EncryptFieldConfig.EncryptMode.ENCRYPT_ONLY);
            fieldModeMap.put("bookingUserEmail", TrpFlinkTransferUtils.EncryptFieldConfig.EncryptMode.ENCRYPT_ONLY);
            fieldModeMap.put("frequentFlyerCardNo", TrpFlinkTransferUtils.EncryptFieldConfig.EncryptMode.ENCRYPT_ONLY);
            fieldModeMap.put("idNumber", TrpFlinkTransferUtils.EncryptFieldConfig.EncryptMode.DECRYPT_AND_ENCRYPT);
            fieldModeMap.put("contactPhone", TrpFlinkTransferUtils.EncryptFieldConfig.EncryptMode.DECRYPT_AND_ENCRYPT);
            
            TrpFlinkTransferUtils.EncryptFieldConfig encryptConfig = new TrpFlinkTransferUtils.EncryptFieldConfig(
                    fieldModeMap,
                    Constants.TRP_AES_KEY,
                    Constants.SM4_KEY
            );

            logger.info("步骤2: 使用Flink SQL模式处理数据（含自动加解密）...");

            // 步骤3：使用SQL + UDF方式处理数据（自动AES解密 → SM4加密）
            TrpFlinkTransferUtils.processDataFromHdfsWithSQL(
                    hdfsFilePath,
                    TOdsScSaleDetail.class,
                    fileDate,
                    "sc_b2c_report_sale",
                    encryptConfig  // 传入加密配置
            );

            logger.info("SC销售报表明细数据转储完成（SQL模式）！");

        } catch (Exception e) {
            logger.error("SC销售报表明细数据转储异常: {}", e.getMessage(), e);
        }
    }

    private static String convertToFileDateFormat(String dateStr) {
        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        DateTimeFormatter fileFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate date = LocalDate.parse(dateStr, inputFormatter);
        return date.format(fileFormatter);
    }
}