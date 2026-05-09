package com.travelsky.trp.usercenter.data.analysis.utils;

import com.travelsky.dataplatform.module.ods.trp.*;
import com.travelsky.dataplatform.module.ods.yxw.TOdsYxwSaleReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * TRP Excel数据转储工具类
 * 支持从FTP下载Excel文件并批量导入到ODS数据库
 */
public class FTPConfigUtils {

    private static final Logger logger = LoggerFactory.getLogger(FTPConfigUtils.class);

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

    }

    /**
     * FTP配置类
     */
    public static class FtpConfig implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        
        private String host;
        private String username;
        private String port;
        private String password;
        private String remoteDirectory;

        public FtpConfig(String host, String username, String password, String remoteDirectory) {
            this.host = host;
            this.username = username;
            this.port = "21";
            this.password = password;
            this.remoteDirectory = remoteDirectory;
        }

        public FtpConfig(String host, String username,String port, String password, String remoteDirectory) {
            this.host = host;
            this.username = username;
            this.port = port;
            this.password = password;
            this.remoteDirectory = remoteDirectory;
        }

        // Getters
        public String getHost() { return host; }
        public String getUsername() { return username; }
        public String getPort() { return port; }
        public String getPassword() { return password; }
        public String getRemoteDirectory() { return remoteDirectory; }
    }

    /**
     * SFTP配置类
     */
    public static class SftpConfig implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        
        private String host;
        private int port;
        private String username;
        private String password;
        private String remoteDirectory;

        public SftpConfig(String host, int port, String username, String password, String remoteDirectory) {
            this.host = host;
            this.port = port;
            this.username = username;
            this.password = password;
            this.remoteDirectory = remoteDirectory;
        }

        public SftpConfig(String host, String username, String password, String remoteDirectory) {
            this(host, 22, username, password, remoteDirectory);
        }

        // Getters
        public String getHost() { return host; }
        public int getPort() { return port; }
        public String getUsername() { return username; }
        public String getPassword() { return password; }
        public String getRemoteDirectory() { return remoteDirectory; }
    }

}