package com.travelsky.dataplatform.main.ods.v2.trp;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.trp.TOdsScInsuranceSaleDetail;
import com.travelsky.trp.usercenter.data.analysis.utils.FTPConfigUtils;
import com.travelsky.trp.usercenter.data.analysis.utils.TrpFlinkTransferUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * SC保险销售明细数据转储脚本（SQL模式 - 推荐）
 * 使用 StreamTableEnvironment + SQL + UDF 自动化处理加解密
 * 从SFTP下载sc_b2c_report_insurance_sell_YYYYMMDD.xlsx文件并导入到ODS数据库
 * 数据流程：SFTP → HDFS → Flink SQL (AES解密 → SM4加密) → Doris
 * 注意：TRP系统使用的是SFTP协议，不是FTP协议
 */
public class ExtractTOdsScInsuranceSaleDetail {

    private static final Logger logger = LoggerFactory.getLogger(ExtractTOdsScInsuranceSaleDetail.class);

    public static void main(String[] args) {
        try {
            logger.info("开始执行SC保险销售明细数据转储（SQL模式，经HDFS）...");
            
            if (args.length < 1) {
                logger.error("缺少ETL日期参数，格式：yyyy-MM-dd");
                logger.error("使用示例：java -cp xxx.jar ExtractTOdsScInsuranceSaleDetail 2025-10-11");
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

            // 配置加解密模式：
            // - 银行订单号：用TRP密钥解密，无需再加密
            // - 账号用户名：直接用统一视图密钥加密
            // - 证件号码：先用TRP密钥解密，再用统一视图密钥加密
            Map<String, TrpFlinkTransferUtils.EncryptFieldConfig.EncryptMode> fieldModeMap = new HashMap<>();
            fieldModeMap.put("bankOrderNumber", TrpFlinkTransferUtils.EncryptFieldConfig.EncryptMode.DECRYPT_ONLY);
            fieldModeMap.put("accountUsername", TrpFlinkTransferUtils.EncryptFieldConfig.EncryptMode.ENCRYPT_ONLY);
            fieldModeMap.put("idNumber", TrpFlinkTransferUtils.EncryptFieldConfig.EncryptMode.DECRYPT_AND_ENCRYPT);
            
            TrpFlinkTransferUtils.EncryptFieldConfig encryptConfig = new TrpFlinkTransferUtils.EncryptFieldConfig(
                    fieldModeMap,
                    Constants.TRP_AES_KEY,
                    Constants.SM4_KEY
            );

            // 步骤1：SFTP → HDFS（下载并上传）
            String hdfsFilePath = TrpFlinkTransferUtils.downloadFromSftpToHdfs(
                    sftpConfig,
                    "sc_b2c_report_insurance_sell",
                    fileDate
            );
            if (hdfsFilePath == null) {
                logger.error("从SFTP下载文件到HDFS失败，FTP文件未找到，跳过本次处理");
                return;
            }
            logger.info("文件已上传到HDFS: {}", hdfsFilePath);

            // SC保险销售明细表中没有联系人手机号码字段，因此不需要加解密
            // 步骤2：HDFS → Flink SQL → Doris（无加解密处理）
            TrpFlinkTransferUtils.processDataFromHdfsWithSQL(
                    hdfsFilePath,
                    TOdsScInsuranceSaleDetail.class,
                    fileDate,
                    "sc_b2c_report_insurance_sell",
                    encryptConfig  // 传入加密配置
            );

            logger.info("SC保险销售明细数据转储完成！");

        } catch (Exception e) {
            logger.error("SC保险销售明细数据转储异常: {}", e.getMessage(), e);
        }
    }

    private static String convertToFileDateFormat(String dateStr) {
        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        DateTimeFormatter fileFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate date = LocalDate.parse(dateStr, inputFormatter);
        return date.format(fileFormatter);
    }
}