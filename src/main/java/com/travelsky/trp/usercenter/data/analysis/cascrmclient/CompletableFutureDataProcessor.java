package com.travelsky.trp.usercenter.data.analysis.cascrmclient;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.DorisUtils;
import com.travelsky.trp.usercenter.data.analysis.cascrmclient.utils.CaCrmQueryUtils;
import com.travelsky.trp.usercenter.data.analysis.cascrmclient.utils.CertificateTypeConverter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class CompletableFutureDataProcessor {
    private static final Logger logger = LoggerFactory.getLogger(CompletableFutureDataProcessor.class);

    public static void processDataBySerialNo() {
        Connection connection = null;
        Statement statement = null;

        try {
            // 1. 获取数据库连接
            connection = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
            statement = DorisUtils.getStatement(connection);

            // 2. 查询最大序号
            int maxId = getMaxSerialNo(statement);
            logger.info("开始并发处理数据，最大序号: {}", maxId);

            // 3. 配置参数
            int threadCount = 11;                  // 并发线程数
            int batchSize = 200000;                // 每批处理数据量
            int totalBatches = (int) Math.ceil((double) maxId / batchSize);
            int batchesPerThread = (int) Math.ceil((double) totalBatches / threadCount);

            logger.info("总批次: {}, 每个线程处理: {}批次", totalBatches, batchesPerThread);

            // 4. 进度统计
            AtomicInteger completedBatches = new AtomicInteger(0);
            AtomicLong totalProcessed = new AtomicLong(0);
            long startTime = System.currentTimeMillis();

            // 5. 创建所有任务
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            for (int threadId = 0; threadId < threadCount; threadId++) {
                final int threadIndex = threadId;
                final int startBatch = threadId * batchesPerThread + 1;
                final int endBatch = Math.min((threadId + 1) * batchesPerThread, totalBatches);

                if (startBatch > endBatch) {
                    continue; // 该线程没有任务
                }

                // 为每个线程创建异步任务
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        processThreadTasks(threadIndex, startBatch, endBatch, batchSize,
                                         maxId, completedBatches, totalProcessed);
                    } catch (Exception e) {
                        logger.error("线程 {} 处理异常", Thread.currentThread().getName(), e);
                        throw new CompletionException(e);
                    }
                }, createThreadPool(threadId));

                futures.add(future);
            }

            // 6. 等待所有任务完成
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[0])
            );
            // 等待所有任务完成
            allFutures.join();

            // 7. 计算性能指标
            long endTime = System.currentTimeMillis();
            long totalTime = endTime - startTime;
            double recordsPerSecond = totalTime > 0 ?
                (double) totalProcessed.get() / totalTime * 1000 : 0;

            logger.info("========== 处理完成 ==========");
            logger.info("总处理时间: {}秒", totalTime / 1000);
            logger.info("总处理数据量: {}条", totalProcessed.get());
            logger.info("处理速度: {}条/秒", recordsPerSecond);
            logger.info("平均每批次处理时间: {}秒",
                totalTime / 1000.0 / completedBatches.get());

        } catch (Exception e) {
            logger.error("并发处理数据异常", e);
            throw new RuntimeException("数据处理失败", e);
        } finally {
            DorisUtils.close(connection, statement, null);
        }
        System.exit(0);
    }

    /**
     * 查询最大序号
     */
    private static int getMaxSerialNo(Statement statement) throws SQLException {
        String maxIdSql = "SELECT MAX(SERIAL_NO) as max_id FROM " +
                Constants.DWD_DB + ".T_DWD_DEPART_SEG_FACT_CRM";
        ResultSet maxRs = DorisUtils.getDorisResult(statement, maxIdSql);
        int maxId = 0;
        if (maxRs.next()) {
            maxId = maxRs.getInt("max_id");
        }
        DorisUtils.close(null, null, maxRs);
        return maxId;
    }

    /**
     * 处理单个线程的所有任务
     */
    private static void processThreadTasks(int threadId, int startBatch, int endBatch,
                                          int batchSize, int maxId,
                                          AtomicInteger completedBatches,
                                          AtomicLong totalProcessed) {

        logger.info("线程[{}] 开始处理批次 {}-{}", threadId, startBatch, endBatch);

        // 每个线程有自己的数据库连接
        Connection threadConn = null;
        Statement threadStmt = null;

        try {
            threadConn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
            threadStmt = DorisUtils.getStatement(threadConn);

            for (int batchNum = startBatch; batchNum <= endBatch; batchNum++) {
                int startSerialNo = (batchNum - 1) * batchSize + 1;
                int endSerialNo = Math.min(batchNum * batchSize, maxId);

                long batchStartTime = System.currentTimeMillis();

                // 处理单个批次
                int processedCount = processSingleBatch(threadConn, threadStmt,
                    batchNum, startSerialNo, endSerialNo, maxId);

                // 更新统计
                totalProcessed.addAndGet(processedCount);
                completedBatches.incrementAndGet();

                long batchTime = System.currentTimeMillis() - batchStartTime;

                // 每完成一个批次，记录日志
                if (batchNum % 5 == 0 || batchNum == endBatch) {
                    logger.debug("线程[{}] 批次 {} 完成，处理 {} 条，耗时 {}ms",
                            threadId, batchNum, processedCount, batchTime);
                }
            }

            logger.info("线程[{}] 所有批次处理完成，共处理 {} 个批次",
                    threadId, (endBatch - startBatch + 1));

        } catch (Exception e) {
            logger.error("线程[{}] 处理异常", threadId, e);
            throw new RuntimeException(e);
        } finally {
            DorisUtils.close(threadConn, threadStmt, null);
        }
    }

    /**
     * 处理单个批次
     */
    private static int processSingleBatch(Connection conn, Statement stmt,
                                         int batchNum, int startSerialNo, int endSerialNo,
                                         int maxId) throws SQLException {

        String querySql = buildBatchQuerySql(startSerialNo, endSerialNo);
        ResultSet rs = null;
        int processedCount = 0;

        try {
            rs = DorisUtils.getDorisResult(stmt, querySql);
            Multimap<String, String> currentMap = ArrayListMultimap.create();

            // 读取数据到内存
            while (rs != null && rs.next()) {
                String certNo = rs.getString("CERT_NUMBER");
                String idType = rs.getString("CERT_TYPE");

                // 证件类型转换
                idType = CertificateTypeConverter.convertCertificateType(certNo, idType);

                if (StringUtils.isBlank(certNo) || StringUtils.isBlank(idType)) {
                    logger.warn("批次 {}: 证件号或证件类型为空，忽略", batchNum);
                    continue;
                }

                currentMap.put(certNo, idType);
                processedCount++;
            }

            // 如果有数据，进行处理
            if (!currentMap.isEmpty() && processedCount > 0) {
                CaCrmQueryUtils.CrmQueryFun(currentMap, "HSD");

                // 显示进度
                double progress = (double) endSerialNo / maxId * 100;
                if (batchNum % 10 == 0 || processedCount > 50000) {
                    logger.info("批次 {} (序号 {}-{}) 处理完成，数据量: {}，进度: {:.2f}%",
                            batchNum, startSerialNo, endSerialNo, processedCount, progress);
                }
            }

            return processedCount;

        } finally {
            DorisUtils.close(null, null, rs);
        }
    }

    /**
     * 构建批次查询SQL
     */
    private static String buildBatchQuerySql(int startSerialNo, int endSerialNo) {
        return "SELECT CERT_NUMBER, CERT_TYPE " +
               "FROM " + Constants.DWD_DB + ".T_DWD_DEPART_SEG_FACT_CRM " +
               "WHERE SERIAL_NO BETWEEN " + startSerialNo + " AND " + endSerialNo + " " +
               "ORDER BY SERIAL_NO";
    }

    /**
     * 创建线程池（每个线程使用独立的线程池，避免线程饥饿）
     */
    private static ExecutorService createThreadPool(int threadId) {
        return new ThreadPoolExecutor(
            1, 1, // 每个线程池固定1个线程
            60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(50), // 有界队列
            new ThreadFactory() {
                private final AtomicInteger counter = new AtomicInteger(1);
                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r,
                        String.format("data-processor-%d-%d", threadId, counter.getAndIncrement()));
                    thread.setDaemon(false);
                    return thread;
                }
            },
            new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    /**
     * 优化的并行处理版本（使用流式API）
     */
    public static void processDataBySerialNoParallel() {
        try {
            // 1. 获取最大序号
            Connection conn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
            Statement stmt = DorisUtils.getStatement(conn);
            int maxId = getMaxSerialNo(stmt);
            DorisUtils.close(conn, stmt, null);

            logger.info("开始并行流处理，最大序号: {}", maxId);

            // 2. 配置参数
            int batchSize = 100000;
            int totalBatches = (int) Math.ceil((double) maxId / batchSize);

            // 3. 使用并行流处理
            long startTime = System.currentTimeMillis();

            // 创建批次列表
            List<Integer> batchNumbers = new ArrayList<>();
            for (int i = 1; i <= totalBatches; i++) {
                batchNumbers.add(i);
            }

            // 使用并行流处理所有批次
            long totalProcessed = batchNumbers.parallelStream()
                .mapToLong(batchNum -> {
                    try {
                        return processBatchWithParallel(batchNum, batchSize, maxId);
                    } catch (Exception e) {
                        logger.error("批次 {} 处理失败", batchNum, e);
                        return 0;
                    }
                })
                .sum();

            long endTime = System.currentTimeMillis();

            logger.info("并行流处理完成，总数据量: {}，耗时: {}秒",
                    totalProcessed, (endTime - startTime) / 1000);

        } catch (Exception e) {
            logger.error("并行流处理异常", e);
        }
    }

    /**
     * 并行流处理单个批次
     */
    private static long processBatchWithParallel(int batchNum, int batchSize, int maxId) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            conn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
            stmt = DorisUtils.getStatement(conn);

            int startSerialNo = (batchNum - 1) * batchSize + 1;
            int endSerialNo = Math.min(batchNum * batchSize, maxId);

            String querySql = buildBatchQuerySql(startSerialNo, endSerialNo);
            rs = DorisUtils.getDorisResult(stmt, querySql);

            Multimap<String, String> currentMap = ArrayListMultimap.create();
            long count = 0;

            while (rs != null && rs.next()) {
                String certNo = rs.getString("CERT_NUMBER");
                String idType = rs.getString("CERT_TYPE");

                idType = CertificateTypeConverter.convertCertificateType(certNo, idType);

                if (StringUtils.isBlank(certNo) || StringUtils.isBlank(idType)) {
                    continue;
                }

                currentMap.put(certNo, idType);
                count++;
            }

            if (!currentMap.isEmpty()) {
                CaCrmQueryUtils.CrmQueryFun(currentMap, "HSD");
            }

            if (batchNum % 20 == 0) {
                logger.debug("并行批次 {} 完成，处理 {} 条", batchNum, count);
            }

            return count;

        } catch (Exception e) {
            logger.error("并行批次 {} 处理异常", batchNum, e);
            return 0;
        } finally {
            DorisUtils.close(conn, stmt, rs);
        }
    }

    /**
     * 带有异常处理的增强版本
     */
    public static void processDataBySerialNoWithExceptionHandling() {
        Connection connection = null;

        try {
            connection = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
            Statement statement = DorisUtils.getStatement(connection);

            int maxId = getMaxSerialNo(statement);
            DorisUtils.close(null, statement, null);

            int threadCount = 10;
            int batchSize = 100000;
            int totalBatches = (int) Math.ceil((double) maxId / batchSize);

            // 创建 CompletableFuture 列表
            List<CompletableFuture<BatchResult>> futures = new ArrayList<>();

            for (int i = 0; i < threadCount; i++) {
                final int threadIndex = i;
                final int batchesPerThread = (int) Math.ceil((double) totalBatches / threadCount);
                final int startBatch = i * batchesPerThread + 1;
                final int endBatch = Math.min((i + 1) * batchesPerThread, totalBatches);

                if (startBatch > endBatch) continue;

                CompletableFuture<BatchResult> future = CompletableFuture.supplyAsync(() -> {
                    return processBatchesRange(threadIndex, startBatch, endBatch, batchSize, maxId);
                }).exceptionally(ex -> {
                    logger.error("线程[{}] 执行异常", threadIndex, ex);
                    return new BatchResult(threadIndex, 0, 0, false);
                });

                futures.add(future);
            }

            // 等待所有任务完成并收集结果
            List<BatchResult> results = futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());

            // 汇总结果
            long totalProcessed = results.stream().mapToLong(BatchResult::getProcessedCount).sum();
            long totalBatchesProcessed = results.stream().mapToLong(BatchResult::getBatchCount).sum();
            long failedThreads = results.stream().filter(r -> !r.isSuccess()).count();

            logger.info("处理完成统计:");
            logger.info("成功线程数: {}", threadCount - failedThreads);
            logger.info("失败线程数: {}", failedThreads);
            logger.info("总处理批次: {}", totalBatchesProcessed);
            logger.info("总处理数据: {}", totalProcessed);

        } catch (Exception e) {
            logger.error("主流程异常", e);
        } finally {
            DorisUtils.close(connection, null, null);
        }
    }

    /**
     * 批次结果类
     */
    private static class BatchResult {
        private int threadId;
        private long processedCount;
        private long batchCount;
        private boolean success;

        public BatchResult(int threadId, long processedCount, long batchCount, boolean success) {
            this.threadId = threadId;
            this.processedCount = processedCount;
            this.batchCount = batchCount;
            this.success = success;
        }

        public int getThreadId() { return threadId; }
        public long getProcessedCount() { return processedCount; }
        public long getBatchCount() { return batchCount; }
        public boolean isSuccess() { return success; }
    }

    private static BatchResult processBatchesRange(int threadId, int startBatch, int endBatch,
                                                  int batchSize, int maxId) {
        // 实现逻辑
        return new BatchResult(threadId, 0, 0, true);
    }
}
