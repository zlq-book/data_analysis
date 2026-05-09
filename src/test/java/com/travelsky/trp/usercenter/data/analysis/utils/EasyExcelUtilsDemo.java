package com.travelsky.trp.usercenter.data.analysis.utils;

import com.travelsky.dataplatform.module.ods.clk.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class EasyExcelUtilsDemo {
    private static final Logger logger = LoggerFactory.getLogger(EasyExcelUtilsDemo.class);

    public static void main(String[] args) {
        // 测试读取会员数据Excel文件
        testReadMemberDataExcel();
    }

    /**
     * 测试读取会员数据Excel文件
     */
    public static void testReadMemberDataExcel() {
        try {
            String filePath = "src/main/resources/CA_MES_20251010.xlsx";
            logger.info("开始读取Excel文件: {}", filePath);

            Map<String, List<?>> result = EasyExcelUtils.readMemberDataExcel(filePath);

            // 输出各sheet页的数据条数
            List<TOdsClkVipMemberTotal> vipMemberTotalList = (List<TOdsClkVipMemberTotal>) result.get("贵宾会员数量");
            logger.info("贵宾会员数量 sheet 数据条数: {}", vipMemberTotalList != null ? vipMemberTotalList.size() : 0);

            List<TOdsClkMemberUpgrade> memberUpgradeList = (List<TOdsClkMemberUpgrade>) result.get("会员升级");
            logger.info("会员升级 sheet 数据条数: {}", memberUpgradeList != null ? memberUpgradeList.size() : 0);

            List<TOdsClkMemberDevelopment> memberDevelopmentList = (List<TOdsClkMemberDevelopment>) result.get("会员发展");
            logger.info("会员发展 sheet 数据条数: {}", memberDevelopmentList != null ? memberDevelopmentList.size() : 0);

            List<TOdsClkMileageAccumulation> mileageAccumulationList = (List<TOdsClkMileageAccumulation>) result.get("里程累积");
            logger.info("里程累积 sheet 数据条数: {}", mileageAccumulationList != null ? mileageAccumulationList.size() : 0);

            List<TOdsClkMileageConsumption> mileageConsumptionList = (List<TOdsClkMileageConsumption>) result.get("里程消费");
            logger.info("里程消费 sheet 数据条数: {}", mileageConsumptionList != null ? mileageConsumptionList.size() : 0);

            // 输出部分数据示例
            if (vipMemberTotalList != null && !vipMemberTotalList.isEmpty()) {
                logger.info("贵宾会员数量 前3条数据:");
                for (int i = 0; i < Math.min(3, vipMemberTotalList.size()); i++) {
                    TOdsClkVipMemberTotal data = vipMemberTotalList.get(i);
                    logger.info("  日期: {}, 名称: {}, 终白: {}", data.getDate(), data.getName(), data.getFinalWhite());
                }
            }

            logger.info("Excel文件读取测试完成");
        } catch (Exception e) {
            logger.error("测试读取会员数据Excel文件时发生异常: {}", e.getMessage(), e);
        }
    }
}