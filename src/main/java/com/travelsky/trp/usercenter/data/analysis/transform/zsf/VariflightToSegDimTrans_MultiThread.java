package com.travelsky.trp.usercenter.data.analysis.transform.zsf;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.SegDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.VariflightFlightModel;
import com.travelsky.trp.usercenter.data.analysis.utils.FlightCache;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class VariflightToSegDimTrans_MultiThread {

    private static final Logger logger = LoggerFactory.getLogger(VariflightToSegDimTrans_MultiThread.class);

    // 将 Object 转为 String，支持 null
    private static String toStringSafe(Object obj) {
        return obj != null ? obj.toString() : null;
    }

    // 将 dateTime 转为 LocalDateTime 字符串
    private static String formatDateTimeSafe(Object ts) {
        if (ts != null) {
            LocalDateTime s = (LocalDateTime) ts;
            return s.toString().replace("T", " ");
        }
        return null;
    }

    /**
     * 安全解析日期字符串，支持标准格式和非标准格式
     * @param dateStr 日期字符串
     * @return LocalDate 对象，解析失败返回 null
     */
    private static LocalDate parseDateSafe(String dateStr) {
        if (dateStr == null || dateStr.trim().isEmpty()) {
            return null;
        }
        try {
            // 先尝试标准格式 yyyy-MM-dd
            return LocalDate.parse(dateStr);
        } catch (Exception e1) {
            try {
                // 兼容非标准格式 yyyy-M-d
                DateTimeFormatter flexFormatter = DateTimeFormatter.ofPattern("yyyy-M-d");
                return LocalDate.parse(dateStr, flexFormatter);
            } catch (Exception e2) {
                logger.error("日期解析失败: {}", dateStr, e2);
                return null;
            }
        }
    }

    /**
     * 计算航班延误状态
     * @return 延误代码: CGYW(出港延误) / JGYW(进港延误) / ZHCH(正常) / null(无法判断)
     */
    private static String calculateDelayStatus(LocalDateTime flightOutgateTime, LocalDateTime plannedDepartureTime,
                                               LocalDateTime flightIngateTime, LocalDateTime plannedArrivalTime) {
        // 判断出港延误
        if (flightOutgateTime != null && plannedDepartureTime != null) {
            long departureDelay = Duration.between(plannedDepartureTime, flightOutgateTime).toMinutes();
            if (departureDelay >= 15) {
                return "CGYW"; // 出港延误
            }
        }

        // 判断进港延误
        if (flightIngateTime != null && plannedArrivalTime != null) {
            long arrivalDelay = Duration.between(plannedArrivalTime, flightIngateTime).toMinutes();
            if (arrivalDelay >= 15) {
                return "JGYW"; // 进港延误
            }
        }

        // 如果推出或推入时间为空，无法计算，返回 null
        if (flightOutgateTime == null || flightIngateTime == null) {
            return null;
        }

        return "ZHCH"; // 正常
    }

    // 存储 UP_LOCATION_DIS_LOCATION -> DIST 的映射（将在 open 方法中加载）

    public static void result(StreamTableEnvironment tEnv, String etlDate) throws Exception {
        // 注册表
        tEnv.executeSql(CreateTableSql.DIM_SEG_DIM);
        tEnv.executeSql(CreateTableSql.SC_TB_TICKET_PRICE_ORDER);

        // 查询 Doris 表 - 只查询需要更新的记录（关键字段为空且距离不为0，且AIRLINE不为空）
        String query = "SELECT \n" +
                "    SEGMENT_KEY,\n" +
                "    AIRLINE,\n" +
                "    OPERAT_AIRLINE,\n" +
                "    OPERAT_FLIGHT_NUM,\n" +
                "    DEPARTURE_DATE,\n" +
                "    DEPARTURE_TIME,\n" +
                "    ARRIVAL_DATE,\n" +
                "    ARRIVAL_TIME,\n" +
                "    DURATION,\n" +
                "    AIRTYPE,\n" +
                "    MARKET_AIRLINE,\n" +
                "    MARKET_FLIGHT_NUM,\n" +
                "    DISTANCE_TPM,\n" +
                "    IS_DELAYED,\n" +
                "    IS_CANCELLED,\n" +
                "    IS_CODE_SHARE,\n" +
                "    CREATE_TIME,\n" +
                "    UPDATE_TIME\n" +
                "FROM T_DIM_SEG_DIM \n" +
                "WHERE ((AIRTYPE IS NULL OR AIRTYPE = '') \n" +
                "    AND (DISTANCE_TPM IS NULL OR DISTANCE_TPM <> '0') \n" +
                "    AND AIRLINE IS NOT NULL AND AIRLINE <> '') LIMIT 1000\n";

        Table dorisTable = tEnv.sqlQuery(query);
        DataStream<Row> rowDataStream = tEnv.toChangelogStream(dorisTable);
        rowDataStream.print("Doris Table Data: ");

        // 注意：不再使用 executeAndCollect() 阻塞方式
        // 机场对-距离映射数据将在 RichMapFunction.open() 中通过 JDBC 加载

        // 使用 RichMapFunction 并在 open 中初始化线程池和缓存
        SingleOutputStreamOperator<SegDimModel> mappedStream1 = rowDataStream
                .filter(row -> row != null)
                .map(new RichMapFunction<Row, SegDimModel>() {
                    private transient FlightCache flightCache; // transient 避免序列化
                    private transient ExecutorService executor; // 线程池
                    private transient Map<String, String> upDisToDistMap; // 机场对-距离映射
                    private transient Connection connection; // JDBC 连接
                    
                    // 统计计数器
                    private transient long totalProcessed = 0;
                    private transient long successCount = 0;
                    private transient long failCount = 0;
                    private transient long timeoutCount = 0;
                    private transient long validDataCount = 0;  // 有效数据（成功查询到航班信息）
                    private transient long emptyDataCount = 0;  // 空数据（只有主键和更新时间）

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 1. 初始化线程池（用于异步 API 调用）
                        executor = Executors.newFixedThreadPool(1);
                        int taskId = getRuntimeContext().getIndexOfThisSubtask();
                        int totalTasks = getRuntimeContext().getNumberOfParallelSubtasks();
                        logger.info("Task[{}/{}] 线程池初始化完成，线程数: 5", taskId, totalTasks);
                        
                        // 2. 初始化本地缓存
                        flightCache = new FlightCache();
                        
                        // 3. 初始化 JDBC 连接（查询机场对-距离映射，连接到 ODS_DB）
                        StringBuilder jdbcUrlBuilder = new StringBuilder("jdbc:mysql://")
                                .append(Constants.DORIS_IP)
                                .append(":")
                                .append(Constants.DORIS_PORT)
                                .append("/")
                                .append(Constants.ODS_DB); // 使用 ODS_DB，因为表在 ODS 库中
                        connection = DriverManager.getConnection(
                                jdbcUrlBuilder.toString(),
                                Constants.ODS_USER,
                                Constants.ODS_PWD
                        );
                        
                        // 4. 通过 JDBC 加载机场对-距离映射数据
                        upDisToDistMap = loadAirportDistanceMapping();
                        logger.info("Task[{}/{}] 成功加载机场对-距离映射，共 {} 条数据", taskId, totalTasks, upDisToDistMap.size());
                    }
                    
                    /**
                     * 通过 JDBC 查询机场对-距离映射
                     */
                    private Map<String, String> loadAirportDistanceMapping() throws SQLException {
                        Map<String, String> distMap = new HashMap<>();
                        // 注意：使用 Doris 物理表名，不是 Flink 表名
                        StringBuilder sqlBuilder = new StringBuilder("SELECT UP_LOCATION, DIS_LOCATION, DIST FROM (")
                                .append("SELECT UP_LOCATION, DIS_LOCATION, DIST, ")
                                .append("ROW_NUMBER() OVER (PARTITION BY UP_LOCATION, DIS_LOCATION ORDER BY EX_DATE DESC) AS rn ")
                                .append("FROM ")
                                .append(Constants.ODS_DB)
                                .append(".T_ODS_SC_TB_TICKET_PRICE_ORDER ")
                                .append("WHERE DIST IS NOT NULL") 
                                .append(") t WHERE rn = 1");
                        String sql = sqlBuilder.toString();
                        
                        try (PreparedStatement stmt = connection.prepareStatement(sql);
                             ResultSet rs = stmt.executeQuery()) {
                            while (rs.next()) {
                                String upLocation = rs.getString("UP_LOCATION");
                                String disLocation = rs.getString("DIS_LOCATION");
                                String dist = rs.getString("DIST");
                                if (upLocation != null && disLocation != null) {
                                    StringBuilder keyBuilder = new StringBuilder(upLocation)
                                            .append("_")
                                            .append(disLocation);
                                    distMap.put(keyBuilder.toString(), dist);
                                }
                            }
                        }
                        return distMap;
                    }
                    
                    @Override
                    public void close() throws Exception {
                        // 输出统计信息
                        int taskId = getRuntimeContext().getIndexOfThisSubtask();
                        logger.info("Task[{}] 处理完成统计: 总数={}, 成功={}, 失败={}, 超时={}, 有效数据={}, 空数据={}", 
                                taskId, totalProcessed, successCount, failCount, timeoutCount, validDataCount, emptyDataCount);
                        
                        // 关闭线程池
                        if (executor != null) {
                            executor.shutdown();
                            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                                executor.shutdownNow();
                            }
                        }
                        // 关闭 JDBC 连接
                        if (connection != null) {
                            connection.close();
                        }
                    }
                    @Override
                    public SegDimModel map(Row row) throws Exception {
                        totalProcessed++;
                        String segmentKey = toStringSafe(row.getFieldAs("SEGMENT_KEY"));
                        
                        try {
                            // 使用 CompletableFuture 在线程池中异步处理 API 调用
                            SegDimModel result = processRowAsync(row).get(60, TimeUnit.SECONDS); // 增加到60秒超时
                            
                            // 判断是否处理成功（根据业务逻辑判断）
                            if (result != null && result.getSegmentKey() != null) {
                                successCount++;
                                // 检查是否为有效数据（至少有AIRTYPE或DISTANCE_TPM其中之一）
                                if (result.getAirtype() != null || 
                                    (result.getDistanceTpm() != null && !"0".equals(result.getDistanceTpm()))) {
                                    validDataCount++;
                                } else {
                                    emptyDataCount++;
                                    logger.warn("航段 {} 处理后无有效数据更新", result.getSegmentKey());
                                }
                                
                                if (totalProcessed % 100 == 0) {
                                    logger.info("已处理 {} 条，成功 {}, 失败 {}, 超时 {}, 有效 {}, 空数据 {}", 
                                            totalProcessed, successCount, failCount, timeoutCount, validDataCount, emptyDataCount);
                                }
                            } else {
                                failCount++;
                                logger.warn("处理失败: segmentKey={}", segmentKey);
                            }
                            
                            return result;
                            
                        } catch (TimeoutException e) {
                            timeoutCount++;
                            logger.error("处理超时(60秒): segmentKey={}", segmentKey, e);
                            // 超时也返回基础数据，避免丢失
                            return createEmptySegDimModel(segmentKey);
                            
                        } catch (Exception e) {
                            failCount++;
                            logger.error("处理异常: segmentKey={}", segmentKey, e);
                            // 异常也返回基础数据，避免丢失
                            return createEmptySegDimModel(segmentKey);
                        }
                    }
                    
                    /**
                     * 异步处理单条数据（在线程池中执行）
                     */
                    private CompletableFuture<SegDimModel> processRowAsync(Row row) {
                        return CompletableFuture.supplyAsync(() -> {
                            String threadName = Thread.currentThread().getName();
                            logger.debug("线程[{}] 开始处理航段: {}", threadName, row.getField("SEGMENT_KEY"));
                            String segmentKey = toStringSafe(row.getFieldAs("SEGMENT_KEY"));
                            String airline = toStringSafe(row.getField("AIRLINE"));
                            // 起飞日期 - 使用安全解析方法
                            String departureDateStr = toStringSafe(row.getField("DEPARTURE_DATE"));
                            LocalDate departureDate = parseDateSafe(departureDateStr);
                            if (departureDate == null) {
                                logger.warn("航段 {} 的起飞日期解析失败: {}", segmentKey, departureDateStr);
                                return createEmptySegDimModel(segmentKey);
                            }
                            // 市场方航班号
                            String marketFlightNum = toStringSafe(row.getField("MARKET_FLIGHT_NUM"));
                            // 市场方航司
                            String marketAirline = toStringSafe(row.getField("MARKET_AIRLINE"));
                            // 承运方航司
                            String operatAirline = toStringSafe(row.getField("OPERAT_AIRLINE"));
                            StringBuilder fullFlightNoBuilder = new StringBuilder();
                            if (marketAirline != null) {
                                fullFlightNoBuilder.append(marketAirline);
                            }
                            if (marketFlightNum != null) {
                                fullFlightNoBuilder.append(marketFlightNum);
                            }
                            String fullFlightNo = fullFlightNoBuilder.toString();

                            // 解析出发/到达机场
                            String depAirport = "";
                            String arrAirport = "";
                            if (airline != null && airline.contains("_")) {
                                String[] parts = airline.split("_", 2);
                                if (parts.length == 2) {
                                    depAirport = parts[0];
                                    arrAirport = parts[1];
                                }
                            }
                            // 查询缓存
                            List<VariflightFlightModel> cachedFlights = flightCache.getFlights(depAirport, arrAirport, departureDate);
                            if (cachedFlights == null) {
                                try {
                                    // API 调用已经在线程池中执行，这里直接同步调用即可
                                    List<VariflightFlightModel> apiResult = VariflightRequest.fetchFlightsByDepArr(
                                            String.valueOf(departureDate), depAirport, arrAirport
                                    );
                                    flightCache.putFlights(depAirport, arrAirport, departureDate, apiResult);
                                    cachedFlights = apiResult;
                                    logger.debug("成功从API获取 {} 的航班数据，共 {} 条", depAirport, apiResult != null ? apiResult.size() : 0);
                                } catch (Exception e) {
                                    logger.error("调用飞友API失败: depAirport={}, date={}", depAirport, departureDate, e);
                                    cachedFlights = new ArrayList<>();
                                }
                            }
                            SegDimModel model = new SegDimModel();
                            // 查某个航班
                            VariflightFlightModel matchedFlight = null;
                            if (cachedFlights != null && !fullFlightNo.isEmpty()) {
                                try {
                                    matchedFlight = flightCache.getFlightByNo(fullFlightNo, depAirport, arrAirport, departureDate);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                                if (matchedFlight != null) {
                                    logger.debug("找到航班: {} 对应航段 {}", matchedFlight.getFlightNo(), segmentKey);
                                    // 提取出航班的推出时间和推入时间
                                    LocalDateTime flightOutgateTime = matchedFlight.getFlightOutgateTime();
                                    LocalDateTime flightIngateTime = matchedFlight.getFlightIngateTime();

                                    // 解析计划起飞和到达时间
                                    String departureDateString = toStringSafe(row.getField("DEPARTURE_DATE"));
                                    String departureTimeString = toStringSafe(row.getField("DEPARTURE_TIME"));
                                    String arrivalDateString = toStringSafe(row.getField("ARRIVAL_DATE"));
                                    String arrivalTimeString = toStringSafe(row.getField("ARRIVAL_TIME"));
                                    LocalDateTime plannedDepartureTime = null;
                                    LocalDateTime plannedArrivalTime = null;
                                    // 只在日期和时间都存在的情况下进行解析
                                    if (departureDateString != null && departureTimeString != null) {
                                        try {
                                            StringBuilder depTimeBuilder = new StringBuilder(departureDateString)
                                                    .append("T").append(departureTimeString);
                                            plannedDepartureTime = LocalDateTime.parse(depTimeBuilder.toString());
                                        } catch (Exception e) {
                                            logger.warn("航段 {} 的起飞时间解析失败: {}T{}", segmentKey, departureDateString, departureTimeString);
                                        }
                                    } else {
                                        logger.warn("航段 {} 缺少起飞日期或时间", segmentKey);
                                    }

                                    if (arrivalDateString != null && arrivalTimeString != null) {
                                        try {
                                            StringBuilder arrTimeBuilder = new StringBuilder(arrivalDateString)
                                                    .append("T").append(arrivalTimeString);
                                            plannedArrivalTime = LocalDateTime.parse(arrTimeBuilder.toString());
                                        } catch (Exception e) {
                                            logger.warn("航段 {} 的到达时间解析失败: {}T{}", segmentKey, arrivalDateString, arrivalTimeString);
                                        }
                                    } else {
                                        logger.warn("航段 {} 缺少到达日期或时间", segmentKey);
                                    }

                                    // 计算延误状态
                                    String delayCode = calculateDelayStatus(flightOutgateTime, plannedDepartureTime,
                                            flightIngateTime, plannedArrivalTime);
                                    // 飞机是否延误
                                    model.setIsDelayed(delayCode);
                                    // 飞机型号
                                    model.setAirtype(matchedFlight.getGeneric());
                                    // 航程距离
                                    model.setDistanceTpm(upDisToDistMap.get(airline));
                                    // 航班是否取消
                                    String flightState = matchedFlight.getFlightState();
                                    // 是否共享 市场方航司是否等于承运方航司
                                    model.setIsCodeShare(matchedFlight.getFlightCompany().equals(operatAirline) ? "否" : "是");
                                    model.setIsCancelled(flightState != null && flightState.equals("取消") ? "是" : "否");
                                } else {
                                    logger.debug("缓存中未找到航班: {}", fullFlightNo);
                                    // 未找到航班，设置距离为0表示无效数据
                                    model.setDistanceTpm("0");
                                    logger.warn("航段 {} 未找到匹配航班: {}, depAirport={}, arrAirport={}", 
                                            segmentKey, fullFlightNo, depAirport, arrAirport);
                                }
                            } else {
                                // 没有航班号或缓存为空，设置距离为0
                                model.setDistanceTpm("0");
                                logger.warn("航段 {} 缺少航班号或缓存为空: fullFlightNo={}, cachedFlightsSize={}", 
                                        segmentKey, fullFlightNo, cachedFlights != null ? cachedFlights.size() : 0);
                            }

                            model.setSegmentKey(segmentKey);
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
                            model.setUpdateTime(LocalDateTime.now().format(formatter));

                            return model;
                        }, executor).exceptionally(throwable -> {
                            logger.error("处理航段 {} 时发生异常", row.getField("SEGMENT_KEY"), throwable);
                            String segmentKey = toStringSafe(row.getFieldAs("SEGMENT_KEY"));
                            return createEmptySegDimModel(segmentKey);
                        });
                    }

                    /**
                     * 创建空的 SegDimModel（解析失败时使用）
                     */
                    private SegDimModel createEmptySegDimModel(String segmentKey) {
                        SegDimModel model = new SegDimModel();
                        model.setSegmentKey(segmentKey);
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
                        model.setUpdateTime(LocalDateTime.now().format(formatter));
                        return model;
                    }
                });

        // 写入 Doris
        logger.info("写入 Doris 表: DB={}, Table={}", Constants.DIM_DB, "T_DIM_SEG_DIM");
        StringBuilder dorisFeAddress = new StringBuilder(Constants.DORIS_FE_IP)
                .append(":").append(Constants.DORIS_FE_PORT);
        StringBuilder dorisBeAddress = new StringBuilder(Constants.DORIS_BE_IP)
                .append(":").append(Constants.DORIS_BE_PORT);
        DorisSink<SegDimModel> dorisSink1 = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_SEG_DIM",
                dorisFeAddress.toString(),
                dorisBeAddress.toString(),
                Constants.DIM_USER,
                Constants.DIM_PWD
        );
        mappedStream1.sinkTo(dorisSink1);
        mappedStream1.print("T_DIM_SEG_DIM");
    }
}