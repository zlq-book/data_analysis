package com.travelsky.dataplatform.utils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 用于处理文本文件中的varchar类型，将括号中的长度乘以3
 */
public class DatabaseVarcharLengthUpdater {

    // 匹配varchar(n)格式的正则表达式
    private static final Pattern VARCHAR_PATTERN = Pattern.compile("varchar\\((\\d+)\\)");

    /**
     * 处理指定的文本文件
     * @param inputFilePath 输入文件路径
     * @param outputFilePath 输出文件路径
     * @throws IOException 处理文件时可能抛出的IO异常
     */
    public static void processFile(String inputFilePath, String outputFilePath) throws IOException {
        // 读取文件内容
        String content = new String(Files.readAllBytes(Paths.get(inputFilePath)), StandardCharsets.UTF_8);

        // 处理内容中的varchar长度
        String processedContent = processContent(content);

        // 将处理后的内容写入输出文件
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(outputFilePath), StandardCharsets.UTF_8))) {
            writer.write(processedContent);
        }
    }

    /**
     * 处理文本内容，将所有varchar(n)中的n乘以3
     * @param content 原始文本内容
     * @return 处理后的文本内容
     */
    public static String processContent(String content) {
        Matcher matcher = VARCHAR_PATTERN.matcher(content);
        StringBuffer result = new StringBuffer();

        // 循环匹配并替换
        while (matcher.find()) {
            // 获取括号中的数字
            int originalLength = Integer.parseInt(matcher.group(1));
            // 计算新长度（乘以3）
            int newLength = originalLength * 3;
            // 替换为新的varchar格式
            matcher.appendReplacement(result, "varchar(" + newLength + ")");
        }
        matcher.appendTail(result);

        return result.toString();
    }

    /**
     * 主方法用于测试
     * @param args 命令行参数，第一个为输入文件路径，第二个为输出文件路径
     *  修改前的样例：
     *  CREATE TABLE `T_ODS_ALP_USER_STUDENT_AUTHEN` (
     *   `ID` int NULL COMMENT "主键",
     *   `SCHOOL_NAME` varchar(200) REPLACE_IF_NOT_NULL NULL COMMENT "学校名称,医院名称",
     *   `CREATE_TIME` datetime(3) REPLACE_IF_NOT_NULL NULL COMMENT "创建时间",
     *   `UPDATE_TIME` datetime(3) REPLACE_IF_NOT_NULL NULL COMMENT "更新时间",
     *   `AUTH_TYPE` tinyint REPLACE_IF_NOT_NULL NULL COMMENT "1学生,2医生,3教师",
     *   `AUTHEN_TYPE` tinyint REPLACE_IF_NOT_NULL NULL COMMENT "渠道临时过渡区",
     *   `CUSTOMER_ID` varchar(100) REPLACE_IF_NOT_NULL NULL COMMENT "第三方id",
     *   `ETL_DATE` date REPLACE_IF_NOT_NULL NULL COMMENT "数据ETL日期"
     * ) ENGINE=OLAP
     * AGGREGATE KEY(`ID`)
     * COMMENT '支付宝小程序学生证认证表'
     * DISTRIBUTED BY HASH(`ID`) BUCKETS 10
     * PROPERTIES (
     * "replication_allocation" = "tag.location.default: 3",
     * "min_load_replica_num" = "-1",
     * "is_being_synced" = "false",
     * "storage_medium" = "hdd",
     * "storage_format" = "V2",
     * "inverted_index_storage_format" = "V1",
     * "light_schema_change" = "true",
     * "disable_auto_compaction" = "false",
     * "enable_single_replica_compaction" = "false",
     * "group_commit_interval_ms" = "10000",
     * "group_commit_data_bytes" = "134217728"
     * );
     *
     *  修改后的样例：
     *  CREATE TABLE `T_ODS_ALP_USER_STUDENT_AUTHEN` (
     *   `ID` int NULL COMMENT "主键",
     *   `SCHOOL_NAME` varchar(600) REPLACE_IF_NOT_NULL NULL COMMENT "学校名称,医院名称",
     *   `CREATE_TIME` datetime(3) REPLACE_IF_NOT_NULL NULL COMMENT "创建时间",
     *   `UPDATE_TIME` datetime(3) REPLACE_IF_NOT_NULL NULL COMMENT "更新时间",
     *   `AUTH_TYPE` tinyint REPLACE_IF_NOT_NULL NULL COMMENT "1学生,2医生,3教师",
     *   `AUTHEN_TYPE` tinyint REPLACE_IF_NOT_NULL NULL COMMENT "渠道临时过渡区",
     *   `CUSTOMER_ID` varchar(100) REPLACE_IF_NOT_NULL NULL COMMENT "第三方id",
     *   `ETL_DATE` date REPLACE_IF_NOT_NULL NULL COMMENT "数据ETL日期"
     * ) ENGINE=OLAP
     * AGGREGATE KEY(`ID`)
     * COMMENT '支付宝小程序学生证认证表'
     * DISTRIBUTED BY HASH(`ID`) BUCKETS 10
     * PROPERTIES (
     * "replication_allocation" = "tag.location.default: 3",
     * "min_load_replica_num" = "-1",
     * "is_being_synced" = "false",
     * "storage_medium" = "hdd",
     * "storage_format" = "V2",
     * "inverted_index_storage_format" = "V1",
     * "light_schema_change" = "true",
     * "disable_auto_compaction" = "false",
     * "enable_single_replica_compaction" = "false",
     * "group_commit_interval_ms" = "10000",
     * "group_commit_data_bytes" = "134217728"
     * );
     *
     */
    public static void main(String[] args) {
//        if (args.length != 2) {
//            System.out.println("使用方法: java VarcharLengthUpdater <输入文件路径> <输出文件路径>");
//            return;
//        }
        String inputFilePath = "C:\\Users\\yuzc\\Desktop\\ddl.txt";
        String outputFilePath = "C:\\Users\\yuzc\\Desktop\\ddlnew.txt";

        try {
            processFile(inputFilePath, outputFilePath);
            System.out.println("文件处理完成！");
        } catch (IOException e) {
            System.err.println("处理文件时发生错误: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
