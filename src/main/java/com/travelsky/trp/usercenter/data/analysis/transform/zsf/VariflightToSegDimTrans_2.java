package com.travelsky.trp.usercenter.data.analysis.transform.zsf;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.DateTimeUtils;
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
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class VariflightToSegDimTrans_2 {

    private static final Logger logger = LoggerFactory.getLogger(VariflightToSegDimTrans.class);

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
            long departureDelay = java.time.Duration.between(plannedDepartureTime, flightOutgateTime).toMinutes();
            if (departureDelay >= 15) {
                return "CGYW"; // 出港延误
            }
        }

        // 判断进港延误
        if (flightIngateTime != null && plannedArrivalTime != null) {
            long arrivalDelay = java.time.Duration.between(plannedArrivalTime, flightIngateTime).toMinutes();
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

    // 注意：不再使用静态变量，改为在 RichMapFunction.open() 中通过 JDBC 加载

    public static void result(StreamTableEnvironment tEnv, String etlDate) throws Exception {
        // 注册表
        tEnv.executeSql(CreateTableSql.DIM_SEG_DIM);
        tEnv.executeSql(CreateTableSql.SC_TB_TICKET_PRICE_ORDER);

        // 查询 Doris 表 - 只查询需要更新的记录（关键字段为空且距离不为0）
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
                "WHERE (AIRTYPE IS NULL OR AIRTYPE = '') \n" +
                "    AND OPERAT_AIRLINE = 'SC' \n" +
                "    AND DISTANCE_TPM IS NULL ";
//                "    AND (DISTANCE_TPM IS NULL OR DISTANCE_TPM <> '0') ";
//                "WHERE OPERAT_AIRLINE = 'SC' ";

        Table dorisTable = tEnv.sqlQuery(query);
        DataStream<Row> rowDataStream = tEnv.toChangelogStream(dorisTable);
        rowDataStream.print("Doris Table Data: ");

        // 注意：不再使用 executeAndCollect() 阻塞方式
        // 机场对-距离映射数据将在 RichMapFunction.open() 中通过 JDBC 加载

        // 使用 RichMapFunction 并在 open 中初始化缓存和距离映射
        SingleOutputStreamOperator<SegDimModel> mappedStream1 = rowDataStream
                .filter(row -> row != null && toStringSafe(row.getField("AIRLINE")) != null)
                .map(new RichMapFunction<Row, SegDimModel>() {
                    private transient FlightCache flightCache; // transient 避免序列化
                    private transient Map<String, String> upDisToDistMap; // 机场对-距离映射
                    private transient Connection connection; // JDBC 连接

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        int taskId = getRuntimeContext().getIndexOfThisSubtask();
                        int totalTasks = getRuntimeContext().getNumberOfParallelSubtasks();
                        
                        // 1. 初始化本地缓存
                        flightCache = new FlightCache();
                        
                        // 2. 初始化 JDBC 连接（查询机场对-距离映射，连接到 ODS_DB）
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
                        
                        // 3. 通过 JDBC 加载机场对-距离映射数据
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
                        // 关闭 JDBC 连接
                        if (connection != null) {
                            connection.close();
                        }
                    }
                    @Override
                    public SegDimModel map(Row row) throws Exception {
                        String segmentKey = toStringSafe(row.getFieldAs("SEGMENT_KEY"));
                        String airline = toStringSafe(row.getField("AIRLINE"));
                        // 起飞日期 - 使用安全解析方法
                        String departureDateStr = toStringSafe(row.getField("DEPARTURE_DATE"));
                        LocalDate departureDate = parseDateSafe(departureDateStr);
                        if (departureDate == null) {
                            logger.warn("航段 {} 的起飞日期解析失败: {}", segmentKey, departureDateStr);
                            SegDimModel emptyModel = createEmptySegDimModel(segmentKey);
                            return emptyModel;
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
                                // 调用 API接口 获取数据
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
                                LocalDateTime flightDeptimeDate = matchedFlight.getFlightDeptimeDate();
                                LocalDateTime flightIngateTime = matchedFlight.getFlightIngateTime();
                                LocalDateTime flightArrtimePlanDate = matchedFlight.getFlightArrtimePlanDate();

                                // 计算延误状态
                                String delayCode = calculateDelayStatus(flightOutgateTime, flightDeptimeDate,
                                        flightIngateTime, flightArrtimePlanDate);
                                // 飞机是否延误
                                model.setIsDelayed(delayCode);
                                // 飞机型号
                                model.setAirtype(matchedFlight.getGeneric());
                                // 航程距离
                                model.setDistanceTpm(upDisToDistMap.get(airline));
                                // 航班是否取消
                                String flightState = matchedFlight.getFlightState();
                                model.setIsCancelled(flightState != null && flightState.equals("取消") ? "1" : "0");
                                // 是否共享 市场方航司是否等于承运方航司
                                model.setIsCodeShare(matchedFlight.getFlightCompany().equals(operatAirline) ? "0" : "1");
                            } else {
                                logger.debug("缓存中未找到航班: {}", fullFlightNo);
                                // 未找到航班，设置距离为0表示无效数据
                                model.setDistanceTpm("0");
                            }
                        } else {
                            // 没有航班号或缓存为空，设置距离为0
                            model.setDistanceTpm("0");
                        }

                        model.setSegmentKey(segmentKey);
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
                        model.setUpdateTime(LocalDateTime.now().format(formatter));

                        return model;
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