package com.travelsky.trp.usercenter.data.analysis.utils;


import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingTicFactModel;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class DorisAsyncFirstIssueFunction extends RichAsyncFunction<TickingTicFactModel, TickingTicFactModel> {

    static final Logger logger = LoggerFactory.getLogger(DorisAsyncFirstIssueFunction.class);

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
    public void asyncInvoke(TickingTicFactModel input, ResultFuture<TickingTicFactModel> resultFuture) throws Exception {


        // Step 3: 异步查询 Doris
        CompletableFuture<TickingTicFactModel> future = CompletableFuture.supplyAsync(() -> {
            try (PreparedStatement stmt = connection.prepareStatement(
                    "SELECT COUNT(*) TID_COUNT FROM T_DWD_TICKING_TIC_FACT\n" +
                            "WHERE FK_BOOKING_USER_TID = ?")) {
                // Step 1: 获取原始字段
                String tid = input.getFkBookingUserTid();
                if (null == tid) {
                    // 鲁雁行没有卡号 不存在
                    return null;
                }
                // Step 2: 处理字段（示例：添加前缀 + 转大写）
                stmt.setString(1, tid);
                stmt.setQueryTimeout(10);
                boolean hasRs = stmt.execute(); // 安全执行
                if (hasRs) {
                    ResultSet rs = stmt.getResultSet();
                    if (rs == null) {
                        return null; // 不存在
                    }
                    if (rs.next()) {
                        // 查询成功：补全维度信息
                        Integer count = rs.getInt("TID_COUNT");
                        // 没有数据：首次出票
                        if (count == 0) {
                            input.setAkFirstIssue(true);
                        }
                        return input;
                    } else {
                        return null; // 不存在
                    }
                }
                // 没有返回null
                return null;

            } catch (Exception e) {
                logger.error("是否首次乘机调用异常", e);
                return null; // 查询失败也视为不存在
            }
        }, executor);

        //  在 thenAccept 中调用 resultFuture.complete
        future.thenAccept(result -> {
            if (result != null) {
                resultFuture.complete(Collections.singletonList(result));
            } else {
                resultFuture.complete(Collections.singletonList(input)); // 返回原值
            }
        });
    }

    @Override
    public void timeout(TickingTicFactModel input, ResultFuture<TickingTicFactModel> resultFuture) throws Exception {
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

