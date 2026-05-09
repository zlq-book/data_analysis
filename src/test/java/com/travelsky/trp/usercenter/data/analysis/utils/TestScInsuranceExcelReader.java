package com.travelsky.trp.usercenter.data.analysis.utils;

import com.travelsky.dataplatform.module.ods.trp.TOdsScInsuranceSaleDetail;

import java.io.File;
import java.util.List;

/**
 * SC 保险销售报表明细 Excel 解析测试
 * 用法：
 * 1) 传入完整路径：java TestScInsuranceExcelReader C:/path/sc_insurance_sale_detail.xlsx
 * 2) 不传参：默认在项目资源目录读取 src/main/resources/sc_insurance_sale_detail.xlsx
 */
public class TestScInsuranceExcelReader {

    public static void main(String[] args) {
        String defaultName = "src\\main\\resources\\sc_insurance_sale_detail.xlsx";
        String filePath;
        if (args != null && args.length > 0 && args[0] != null && !args[0].trim().isEmpty()) {
            filePath = args[0].trim();
        } else {
            filePath = new File(defaultName).getAbsolutePath();
        }

        System.out.println("开始解析Excel: " + filePath);
        if (!new File(filePath).exists()) {
            System.out.println("文件不存在: " + filePath);
            System.out.println("请将保险销售报表明细 Excel 文件放在: " + filePath);
            System.exit(1);
        }

        long start = System.currentTimeMillis();
        // 表头从第1行开始（EasyExcel为1-based）
        List<TOdsScInsuranceSaleDetail> list = EasyExcelUtils.readExcelWithHeadRow(
            filePath, TOdsScInsuranceSaleDetail.class, 1
        );
        long cost = System.currentTimeMillis() - start;

        System.out.println("解析完成, 记录数: " + list.size() + ", 耗时(ms): " + cost);

        int preview = Math.min(5, list.size());
        for (int i = 0; i < preview; i++) {
            System.out.println("样例#" + (i + 1) + ": " + list.get(i));
        }
    }
}
