package com.travelsky.trp.usercenter.data.analysis.transform.lyx;


import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class OtiOfiOpiAsync extends RichAsyncFunction<Row, Row> {

    static final Logger logger = LoggerFactory.getLogger(OtiOfiOpiAsync.class);

    private transient Connection connection;
    private transient ExecutorService executor;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化线程池（用于异步查询）
        executor = Executors.newFixedThreadPool(10);

        // 初始化 Doris 连接（Doris 支持 MySQL 协议）
        connection = DriverManager.getConnection(
                "jdbc:mysql://" + Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.DWD_DB,
                Constants.DWD_USER,
                Constants.DWD_PWD
        );

    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {
        // Step 1: 获取时间
        Object createTime = input.getField("CREATE_TIME");
        Object pnr = input.getField("PNR_NO");
        if (null == createTime || null == pnr) {
            // 创建时间 pnr 不存在
            return;
        }
        boolean tomFlag = false;
        boolean yesFlag = false;
        LocalDateTime localDateTime = LocalDateTime.parse(createTime.toString());
        String pnrTime = localDateTime.format(TransUtils.TIME_FORMATTER);
        if (pnrTime.startsWith("00:00:")) {
            // 00：00：00-00：01：00的订单需要T-1
            yesFlag = true;
        }

        if (pnrTime.startsWith("23:00:")) {
            // 23：00：00-00：00：00的订单需要T+1
            tomFlag = true;
        }

        if (!tomFlag && !yesFlag) {
            // 不在时间范围结束 或者 订单类型不是1
            CompletableFuture<Row> future = CompletableFuture.supplyAsync(() -> {
                return input;
            }, executor);


        } else {
            // Step 3: 异步查询 当天 Doris
            CompletableFuture<Integer> future1 = CompletableFuture.supplyAsync(() -> {
                try (PreparedStatement stmt = connection.prepareStatement(
                        "SELECT COUNT(*) RES_COUNT  FROM T_DWD_BOOKING_PNR_FACT a\n" +
                                "WHERE a.PK_ID = ?")) {
                    stmt.setQueryTimeout(10);
                    stmt.setString(1, pnr.toString() + localDateTime.format(TransUtils.DATE_FORMATTER_TWO));
                    boolean hasRs = stmt.execute(); // 安全执行
                    if (hasRs) {
                        ResultSet rs = stmt.getResultSet();
                        if (rs.next()) {
                            // 查询成功：补全维度信息
                            Integer resCount = rs.getInt("RES_COUNT");
                            return resCount;
                        } else {
                            return 0; // 不存在
                        }
                    }
                    // 没有返回null
                    return 0;

                } catch (Exception e) {
                    logger.error("鲁雁行异步调用异常", e);
                    return 0; // 查询失败也视为不存在
                }
            }, executor);

            // 另一天查询
            boolean finalYesFlag = yesFlag;
            CompletableFuture<Integer> future2 = CompletableFuture.supplyAsync(() -> {
                try (PreparedStatement stmt = connection.prepareStatement(
                        "SELECT COUNT(*) RES_COUNT  FROM T_DWD_BOOKING_PNR_FACT a\n" +
                                "WHERE a.PK_ID = ?")) {
                    if (finalYesFlag) {
                        stmt.setString(1, pnr.toString() +
                                localDateTime.minusDays(1l).format(TransUtils.DATE_FORMATTER_TWO));
                    } else {
                        stmt.setString(1, pnr.toString() +
                                localDateTime.plusDays(1l).format(TransUtils.DATE_FORMATTER_TWO));
                    }
                    stmt.setQueryTimeout(10);
                    boolean hasRs = stmt.execute(); // 安全执行
                    if (hasRs) {
                        ResultSet rs = stmt.getResultSet();
                        if (rs.next()) {
                            // 查询成功：补全维度信息
                            Integer resCount = rs.getInt("RES_COUNT");
                            return resCount;
                        } else {
                            return 0; // 不存在
                        }
                    }
                    // 没有返回null
                    return 0;

                } catch (Exception e) {
                    logger.error("鲁雁行异步调用异常", e);
                    return 0; // 查询失败也视为不存在
                }
            }, executor);
            // --- 组合两个异步结果 ---
            // 方法1: 使用 allOf 等待两者完成，然后获取结果
            CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(future1, future2)
                    .thenRun(() -> {
                        try {
                            // 获取两个查询的结果
                            Integer res1 = future1.get(); // get() 不会阻塞，因为 allOf 已完成
                            Integer res2 = future2.get();

                            if (1 == res1) {
                                // 当天查询到
                                // 创建输出 Row
                                Row output = Row.copy(input);
                                // 将结果发送出去
                                resultFuture.complete(Collections.singletonList(output));
                            } else if (1 == res2) {
                                // 另一天查询到
                                Row output = Row.copy(input);
                                if (finalYesFlag) {
                                    output.setField("CREATE_TIME",
                                            localDateTime.minusDays(1l).toString());
                                } else {
                                    output.setField("CREATE_TIME",
                                            localDateTime.plusDays(1l).toString());
                                }
                                // 将结果发送出去
                                resultFuture.complete(Collections.singletonList(output));
                            } else {
                                // 查询无结果
                                Row output = Row.copy(input);
                                if (finalYesFlag) {
                                    output.setField("CREATE_TIME",
                                            localDateTime.minusDays(1l).toString());
                                } else {
                                    output.setField("CREATE_TIME",
                                            localDateTime.plusDays(1l).toString());
                                }
                                Row output1 = Row.copy(input);

                                // 将结果发送出去
                                resultFuture.complete(Collections.unmodifiableList(
                                        Arrays.asList(output, output1)));
                            }


                        } catch (Exception e) {
                            // 处理获取结果时的异常 (理论上不会发生，因为 allOf 成功)
                            resultFuture.completeExceptionally(e);
                        }
                    });

            // --- 处理组合过程中的异常 ---
            // 如果 future1 或 future2 失败，combinedFuture 也会失败
            combinedFuture.exceptionally(throwable -> {
                // 记录日志 (生产环境)
                 logger.error("异步查询失败", throwable);
                resultFuture.completeExceptionally(throwable);
                return null;
            });

        }




    }

    @Override
    public void timeout(Row input, ResultFuture<Row> resultFuture) throws Exception {
        // 可选：处理超时，比如返回 null 或打日志
        resultFuture.complete(Collections.emptyList());
    }

    @Override
    public void close() throws Exception {
        if (executor != null) {
            executor.shutdown();
        }
        if (connection != null) {
            connection.close();
        }
    }
}

