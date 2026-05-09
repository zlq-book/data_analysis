package com.travelsky.trp.usercenter.data.analysis.utils;

import com.travelsky.dataplatform.constans.Constants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 异步删除处理
 * @author TS.SHA.fuhuazhang
 * @date 2025/8/5  09:40
 */
public class DorisAsyncDeleteFunction extends RichSinkFunction<String> {

    private static final Logger logger = LoggerFactory.getLogger(DorisAsyncDeleteFunction.class);

    private transient Connection connection;
    private static final Object LOCK = new Object();
    private transient ExecutorService executor;
    private transient List<String> batchBuffer;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        executor = Executors.newFixedThreadPool(5);
        connection = DriverManager.getConnection(
                "jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.DWD_DB,
                Constants.DWD_USER,
                Constants.DWD_PWD
        );
        connection.setAutoCommit(false);

        batchBuffer = new ArrayList<>();
    }

    @Override
    public void invoke(String pkId, Context context) throws Exception {
        synchronized (LOCK) {
            batchBuffer.add(pkId);
        }
        int batchSize = 100;
        if (batchBuffer.size() >= batchSize) {
            triggerAsyncDelete();
        }
    }

    private void triggerAsyncDelete() {
        List<String> taskBatch;
        synchronized (LOCK) {
            if (batchBuffer.isEmpty()) return;
            taskBatch = new ArrayList<>(batchBuffer);
            batchBuffer.clear();
        }

        CompletableFuture.runAsync(() -> {
            try {
                executeDeleteBatch(taskBatch);
                logger.info("Deleted {} records", taskBatch.size());
            } catch (Exception e) {
                logger.error("Delete failed", e);
                handleDeleteFailure(taskBatch, e);
            }
        }, executor);
    }

    /**
     * 执行一批 DELETE 操作
     */
    private void executeDeleteBatch(List<String> pkIds) throws Exception {
        String sql = "DELETE FROM " + Constants.DWD_DB + ".T_DWD_BOOKING_SEG_FACT WHERE PK_ID = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            for (String pkId : pkIds) {
                stmt.setString(1, pkId);
                stmt.addBatch();
            }
            stmt.executeBatch();
            connection.commit(); // 提交事务
        }
    }

    /**
     * 处理删除失败（可扩展：重试、写入 Kafka 错误队列等）
     */
    private void handleDeleteFailure(List<String> failedBatch, Exception e) {
        // 示例：简单重试一次
        try {
            Thread.sleep(1000);
            executeDeleteBatch(failedBatch);
            logger.info("Retry succeeded for {} records", failedBatch.size());
        } catch (Exception ex) {
            logger.error("Retry failed for {} records", failedBatch.size(), ex);
        }
    }

    @Override
    public void close() throws Exception {
        // 关闭前处理剩余数据
        if (!batchBuffer.isEmpty()) {
            triggerAsyncDelete();
            // 注意：这里无法完全等待异步任务完成，生产环境建议用 CountDownLatch 或优雅关闭
            Thread.sleep(2000); // 等待最后一批完成（不完美，但简单）
        }

        if (executor != null) {
            executor.shutdown();
            if (!executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
        if (connection != null) {
            connection.close();
        }
    }
}
