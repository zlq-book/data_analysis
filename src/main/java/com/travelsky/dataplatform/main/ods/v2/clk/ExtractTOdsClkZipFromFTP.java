package com.travelsky.dataplatform.main.ods.v2.clk;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.yxw.TOdsYxwSaleReport;
import com.travelsky.dataplatform.utils.KerberosAuthUtils;
import com.travelsky.trp.usercenter.data.analysis.utils.FTPConfigUtils;
import com.travelsky.trp.usercenter.data.analysis.utils.FTPUtils;
import com.travelsky.trp.usercenter.data.analysis.utils.SFTPUtils;
import com.travelsky.trp.usercenter.data.analysis.utils.TrpFlinkTransferUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * 常客ftp数据（CLK）
 * 完整流程：SFTP → FTP →HDFS
 * 文件格式：SC202505.zip
 * 数据源：常客ftp /CA 目录
 * 目标：hdfs://cmss/data/test/encrypt/clk_xml_upload
 * 
 * 使用方式：
 * 1. 单日处理：java ExtractTOdsYxwSaleReportFromFTP 20250115
 * 2. 批量处理：java ExtractTOdsYxwSaleReportFromFTP 20250115 20250120
 */
public class ExtractTOdsClkZipFromFTP {

    private static final Logger logger = LoggerFactory.getLogger(ExtractTOdsClkZipFromFTP.class);

    public static void main(String[] args) {
        try {
            logger.info("常客ftp（CLK）...");
            
            // 1. 参数校验
            if (args.length < 1) {
                logger.error("缺少ETL日期参数");
                return;
            }
            String etlDate = args[0];
//            String etlDate = "2025-10-11";
            // 2. 解析日期参数，
            if (etlDate.length() <10) {
                logger.error("参数格式不正确");
                return;
            }
            String destFolder = "/tmp/" ;
            String dealDate = etlDate.replace("-","").substring(0,6);

            // 取4种之一 ACCU 类型进行校验 分为原始 和 处理完
            String oriName = "ACCU" + dealDate + ".xml";
            String doneName = "ACCU" + dealDate + "done.xml";
            String oriPath = Constants.CLK_FILE_PATH + "/" + oriName;
            String donePath = Constants.CLK_FILE_PATH + "/" + doneName;
            Configuration conf = new Configuration();
            try {
                conf.set("fs.defaultFS", "hdfs://cmss");
                conf.set("dfs.nameservices", "cmss");
                conf.set("dfs.ha.namenodes.cmss", "nn1,nn2");
                conf.set("dfs.namenode.rpc-address.cmss.nn1", "emr-master-001.novalocal:8020");
                conf.set("dfs.namenode.rpc-address.cmss.nn2", "emr-master-ha-001.novalocal:8020");
                conf.set("dfs.client.failover.proxy.provider.cmss",
                        "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
//            KerberosAuthUtils.initKerberosAuth(Constants.KRB5_CONF_PATH, Constants.SYS_UCV_TEST_FOR_MAPREDUCE_PRINCIPAL, Constants.SYS_UCV_TEST_FOR_MAPREDUCE_KEYTAB_PATH);


            } catch (Exception e) {
                e.printStackTrace();
            }
            FileSystem fs = FileSystem.get(conf);

            // hdfs文件是否存在
            boolean oriExists = fs.exists(new Path(oriPath));
            boolean doneExists = fs.exists(new Path(donePath));
            if (oriExists || doneExists) {
                // 文件存在 不需要下载结束 结束
                fs.close();
                logger.info("文件存在 任务结束");
                return;
            }
            fs.close();




            // 3. 配置SFTP连接信息（CLK FTP）
            FTPConfigUtils.FtpConfig ftpConfig = new FTPConfigUtils.FtpConfig(
                    Constants.CLK_MILE_FTP_HOST,
                    Constants.CLK_MILE_FTP_USERNAME,
                    Constants.CLK_MILE_FTP_PORT,
                    Constants.CLK_MILE_FTP_PWD,
                    "/CA"  // zip存放目录
            );
            // 创建连接
            FTPUtils ftpUtils = new FTPUtils(ftpConfig.getHost(), ftpConfig.getPort(),
                    ftpConfig.getUsername(), ftpConfig.getPassword());
            String targetFile = "SC" + dealDate + ".zip";
            if (!ftpUtils.fileExists(ftpConfig.getRemoteDirectory() + "/" + targetFile)) {
                logger.info("常客ftp 文件不存在结束");
                if (null != ftpUtils) {
                    ftpUtils.disconnect();
                }
                return;
            }
            try {
                ftpUtils.connect();
                // 下载文件
                ftpUtils.downloadFile(ftpConfig.getRemoteDirectory() + "/" + targetFile,
                        destFolder + targetFile);
            } catch (Exception e) {
                logger.info("常客ftp sftp连接异常:{}",e.getMessage());
            } finally {
                if (null != ftpUtils) {
                    ftpUtils.disconnect();
                }
            }

            // 5.解压文件
            logger.info("======================================");
            logger.info("步骤: 解压zip文件");
            unzip(destFolder + targetFile,destFolder );

            logger.info("解压完成");

            // 5. 上传hdfs
            logger.info("======================================");
            logger.info("步骤: 上传hdfs...");
            FileSystem fileSystem = FileSystem.get(conf);
            if (!uploadFfile(fileSystem, destFolder, "ACCU" + dealDate + ".xml")) {
                return;
            }
            if (!uploadFfile(fileSystem, destFolder, "ACCUINT" + dealDate + ".xml")) {
                return;
            }
            if (!uploadFfile(fileSystem, destFolder, "EXCH" + dealDate + ".xml")) {
                return;
            }
            if (!uploadFfile(fileSystem, destFolder, "EXCHINT" + dealDate + ".xml")) {
                return;
            }
            fileSystem.close();
            logger.info("======================================");
            logger.info("上传完成！");

        } catch (Exception e) {
            logger.error("常客ftp 转储异常: {}", e.getMessage(), e);
            return;
        }
    }

    public static boolean uploadFfile(FileSystem fileSystem, String destFolder, String uploadName) {
        try {
            File localDir = new File(destFolder + uploadName);
            if (!localDir.exists()) {
                logger.info("本地文件不存在: ");
                fileSystem.close();
                return false;
            }

            // 确保 HDFS 目标目录存在
            Path hdfsDest = new Path(Constants.CLK_FILE_PATH);
            if (fileSystem.exists(hdfsDest)) {
                // 开始上传
                Path src = new Path(destFolder  + uploadName);
                Path dst = new Path(Constants.CLK_FILE_PATH + "/" + uploadName); // 明确指定目标文件名
                fileSystem.copyFromLocalFile(
                        true,   // 删除本地源文件？false=保留
                        true,   // 覆盖目标？true=覆盖
                        src,
                        dst
                );

            }

        } catch (IOException e) {
            logger.error("常客ftp 上传异常: {}", e.getMessage(), e);
            return false;
        }
        return true;
    }

    /**
     * 解压 ZIP 文件到指定目录
     *
     * @param zipFilePath   ZIP 文件路径（如 "D:/data/archive.zip"）
     * @param destDirPath   解压目标目录（如 "D:/output/"）
     * @throws IOException
     */
    public static void unzip(String zipFilePath, String destDirPath) throws IOException {
        java.nio.file.Path destDir = java.nio.file.Paths.get(destDirPath);
        if (!Files.exists(destDir)) {
            Files.createDirectories(destDir);
        }

        try (ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFilePath), StandardCharsets.UTF_8)) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                java.nio.file.Path entryPath = destDir.resolve(entry.getName());

                // ⚠️ 防止 ZIP 路径遍历攻击（如 ../../etc/passwd）
                if (!entryPath.normalize().startsWith(destDir.normalize())) {
                    System.err.println("非法 ZIP 条目，跳过: " + entry.getName());
                    continue;
                }

                if (entry.isDirectory()) {
                    Files.createDirectories(entryPath);
                } else {
                    Files.createDirectories(entryPath.getParent()); // 确保父目录存在
                    Files.copy(zis, entryPath, StandardCopyOption.REPLACE_EXISTING);
                }
                zis.closeEntry();
            }
        }
    }
}
