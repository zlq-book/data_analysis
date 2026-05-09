package com.travelsky.trp.usercenter.data.analysis.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * TRP数据转储配置管理类
 * 集中管理文件前缀、FTP路径等配置信息
 */
public class TrpDataTransferConfig {

    /**
     * 文件前缀配置映射
     * Key: 实体类简名, Value: Excel文件前缀
     */
    public static final Map<String, String> FILE_PREFIX_MAP = new HashMap<>();
    
    /**
     * FTP目录配置映射
     * Key: 实体类简名, Value: FTP远程目录路径
     */
    public static final Map<String, String> FTP_DIRECTORY_MAP = new HashMap<>();

    static {
        // SC销售报表
        FILE_PREFIX_MAP.put("TOdsScSaleDetail", "sc_b2c_report_sale");
        FTP_DIRECTORY_MAP.put("TOdsScSaleDetail", "/upload/sc/sale/");

        // SC机票退票报表
        FILE_PREFIX_MAP.put("TOdsScTicketRefundDetail", "sc_b2c_report_air_refund");
        FTP_DIRECTORY_MAP.put("TOdsScTicketRefundDetail", "/upload/sc/refund/");

        // SC保险销售报表
        FILE_PREFIX_MAP.put("TOdsScInsuranceSaleDetail", "sc_b2c_report_insurance_sell");
        FTP_DIRECTORY_MAP.put("TOdsScInsuranceSaleDetail", "/upload/sc/insurance/");

        // SC保险退款报表
        FILE_PREFIX_MAP.put("TOdsScInsuranceRefundDetail", "sc_b2c_report_insurance_refund");
        FTP_DIRECTORY_MAP.put("TOdsScInsuranceRefundDetail", "/upload/sc/insurance/");

        // SC卡券销售报表
        FILE_PREFIX_MAP.put("TOdsScCouponSaleDetail", "sc_b2c_report_coupon_sell");
        FTP_DIRECTORY_MAP.put("TOdsScCouponSaleDetail", "/upload/sc/coupon/");

        // SC卡券退款报表
        FILE_PREFIX_MAP.put("TOdsScCouponRefundDetail", "sc_b2c_report_coupon_refund");
        FTP_DIRECTORY_MAP.put("TOdsScCouponRefundDetail", "/upload/sc/coupon/");
    }

    /**
     * 根据实体类获取文件前缀
     * @param entityClass 实体类
     * @return 文件前缀
     */
    public static String getFilePrefix(Class<?> entityClass) {
        return FILE_PREFIX_MAP.get(entityClass.getSimpleName());
    }

    /**
     * 根据实体类获取FTP目录
     * @param entityClass 实体类
     * @return FTP目录路径
     */
    public static String getFtpDirectory(Class<?> entityClass) {
        return FTP_DIRECTORY_MAP.get(entityClass.getSimpleName());
    }

    /**
     * 检查实体类是否支持
     * @param entityClass 实体类
     * @return 是否支持
     */
    public static boolean isSupported(Class<?> entityClass) {
        return FILE_PREFIX_MAP.containsKey(entityClass.getSimpleName());
    }

    /**
     * 获取所有支持的实体类名称
     * @return 实体类名称集合
     */
    public static java.util.Set<String> getSupportedEntityNames() {
        return FILE_PREFIX_MAP.keySet();
    }

    /**
     * 数据转储任务配置
     */
    public static class TaskConfig {
        private final Class<?> entityClass;
        private final String filePrefix;
        private final String ftpDirectory;
        private final String description;

        public TaskConfig(Class<?> entityClass, String description) {
            this.entityClass = entityClass;
            this.filePrefix = TrpDataTransferConfig.getFilePrefix(entityClass);
            this.ftpDirectory = TrpDataTransferConfig.getFtpDirectory(entityClass);
            this.description = description;
        }

        // Getters
        public Class<?> getEntityClass() { return entityClass; }
        public String getFilePrefix() { return filePrefix; }
        public String getFtpDirectory() { return ftpDirectory; }
        public String getDescription() { return description; }

        @Override
        public String toString() {
            return String.format("TaskConfig{entityClass=%s, filePrefix='%s', ftpDirectory='%s', description='%s'}",
                    entityClass.getSimpleName(), filePrefix, ftpDirectory, description);
        }
    }
}