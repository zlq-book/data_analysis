package com.travelsky.dataplatform.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
/**
 * @author kuangaihua
 * @date 2025/7/14 10:24
 */


public class WholeFileSource implements SourceFunction<String> {
    private String filePath;
    private volatile boolean isRunning = true;

    public WholeFileSource(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        synchronized (ctx.getCheckpointLock()) {
            try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath), StandardCharsets.UTF_8)) {
                StringBuilder contentBuilder = new StringBuilder();
                String currentLine;

                while ((currentLine = reader.readLine()) != null) {
                    contentBuilder.append(currentLine).append("\n");
                }
                // 发出整个文件的内容作为一个单一的字符串
                ctx.collect(contentBuilder.toString());
            } catch (IOException e) {
                System.err.println("Error reading file: " + e.getMessage());
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}


