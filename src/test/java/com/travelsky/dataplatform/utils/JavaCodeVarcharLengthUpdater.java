package com.travelsky.dataplatform.utils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 用于将Java文件中的varchar(n)替换为varchar(3n)的工具类
 */
public class JavaCodeVarcharLengthUpdater {
    // 匹配varchar(n)的正则表达式，考虑可能的空格和不同大小写
    private static final Pattern VARCHAR_PATTERN = Pattern.compile(
            "varchar\\s*\\(\\s*(\\d+)\\s*\\)",
            Pattern.CASE_INSENSITIVE
    );

    /**
     * 处理指定目录下的所有Java文件
     * @param directoryPath 目录路径
     * @throws IOException 如果处理过程中发生IO错误
     */
    public void processDirectory(String directoryPath) throws IOException {
        Path directory = Paths.get(directoryPath);

        if (!Files.isDirectory(directory)) {
            throw new IllegalArgumentException("指定路径不是一个有效的目录: " + directoryPath);
        }

        // 递归遍历目录中的所有文件
        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                // 只处理.java文件
                if (file.getFileName().toString().endsWith(".java")) {
                    processJavaFile(file);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                System.err.println("无法访问文件: " + file + ", 错误: " + exc.getMessage());
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * 处理单个Java文件，替换其中的varchar(n)为varchar(3n)
     * @param javaFile Java文件路径
     * @throws IOException 如果处理过程中发生IO错误
     */
    private void processJavaFile(Path javaFile) throws IOException {
        System.out.println("处理文件: " + javaFile);

        // 创建临时文件用于写入修改后的内容
        Path tempFile = Files.createTempFile(javaFile.getParent(), "temp_", ".java");

        try (BufferedReader reader = Files.newBufferedReader(javaFile, StandardCharsets.UTF_8);
             BufferedWriter writer = Files.newBufferedWriter(tempFile, StandardCharsets.UTF_8)) {

            String line;
            while ((line = reader.readLine()) != null) {
                // 替换当前行中的varchar(n)为varchar(3n)
                String modifiedLine = replaceVarchar(line);
                writer.write(modifiedLine);
                writer.newLine();
            }
        }

        // 创建原文件的备份
//        Path backupFile = Paths.get(javaFile.toString() + ".bak");
//        Files.move(javaFile, backupFile, StandardCopyOption.REPLACE_EXISTING);

        // 将临时文件移动到原文件位置
        Files.move(tempFile, javaFile, StandardCopyOption.REPLACE_EXISTING);

//        System.out.println("已处理并创建备份: " + backupFile);
    }

    /**
     * 替换一行中的varchar(n)为varchar(3n)
     * @param line 待处理的行
     * @return 处理后的行
     */
    private String replaceVarchar(String line) {
        Matcher matcher = VARCHAR_PATTERN.matcher(line);
        StringBuffer sb = new StringBuffer();

        while (matcher.find()) {
            // 获取数字部分
            String numberStr = matcher.group(1);
            try {
                int number = Integer.parseInt(numberStr);
                int tripled = number * 3;
                // 替换为varchar(3n)，保持原有的大小写风格
                String originalVarchar = matcher.group(0).substring(0,
                        matcher.group(0).indexOf('(')).trim();
                matcher.appendReplacement(sb, originalVarchar + "(" + tripled + ")");
            } catch (NumberFormatException e) {
                // 如果不是有效的数字，不进行替换
                matcher.appendReplacement(sb, matcher.group(0));
            }
        }
        matcher.appendTail(sb);

        return sb.toString();
    }

    /**
     * 主方法，用于测试工具类
     * @param args 命令行参数，第一个参数为要处理的目录路径
     */
    public static void main(String[] args) {

        String directoryPath = "D:\\Allsources\\trp-usercenter_v1_usercenter_data_analysis\\src\\main\\java\\com\\travelsky\\dataplatform\\main\\ods";

        JavaCodeVarcharLengthUpdater replacer = new JavaCodeVarcharLengthUpdater();
        try {
            replacer.processDirectory(directoryPath);
            System.out.println("处理完成");
        } catch (Exception e) {
            System.err.println("处理过程中发生错误: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

