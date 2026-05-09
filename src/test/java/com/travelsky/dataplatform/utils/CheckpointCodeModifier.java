package com.travelsky.dataplatform.utils;


import java.io.*;
import java.nio.file.*;
import java.util.stream.*;
import java.util.regex.*;

public class CheckpointCodeModifier {
    private static final String TARGET_METHOD = "CheckpointUtils.setCheckpoint(env)";
    private static final Pattern CLASS_NAME_PATTERN = Pattern.compile("class\\s+(\\w+)");

    public static void main(String[] args) throws IOException {
        Path startDir = Paths.get("D:\\myjava\\sda\\trp-usercenter_v1_usercenter_data_analysis\\src\\main\\java\\com\\travelsky"); // 修改为您的源码目录
        modifyFiles(startDir);
    }

    private static void modifyFiles(Path dir) throws IOException {
        try (Stream<Path> paths = Files.walk(dir)) {
            paths.filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".java"))
                    .forEach(p -> {
                        try {
                            String content = new String(Files.readAllBytes(p));
                            String className = extractClassName(content);

                            if (className != null) {
                                String newContent = content.replace(
                                        TARGET_METHOD,
                                        "CheckpointUtils.setCheckpoint(env, \"" + className + "\")"
                                );

                                if (!newContent.equals(content)) {
                                    Files.write(p, newContent.getBytes());
                                    System.out.println("Modified: " + p);
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("Error processing " + p + ": " + e.getMessage());
                        }
                    });
        }
    }

    private static String extractClassName(String fileContent) {
        Matcher matcher = CLASS_NAME_PATTERN.matcher(fileContent);
        return matcher.find() ? matcher.group(1) : null;
    }
}

