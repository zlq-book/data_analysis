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

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VariflightToSegDimTrans_1 {

    private static final Logger logger = LoggerFactory.getLogger(VariflightToSegDimTrans_1.class);

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

    // 存储 UP_LOCATION_DIS_LOCATION -> DIST 的映射（static 确保全局唯一，或在 open 中初始化）
    private static Map<String, String> upDisToDistMap;

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
                "WHERE ((AIRTYPE IS NULL OR AIRTYPE = '') \n" +
                "    AND (DISTANCE_TPM IS NULL OR DISTANCE_TPM <> '0')) \n";

        Table dorisTable = tEnv.sqlQuery(query);
        DataStream<Row> rowDataStream = tEnv.toChangelogStream(dorisTable);
        rowDataStream.print("Doris Table Data: ");


        String TICKET_PRICE_ORDER = "SELECT \n" +
                "    UP_LOCATION,\n" +
                "    DIS_LOCATION,\n" +
                "    DIST,\n" +
                "    EX_DATE\n" +
                "FROM (\n" +
                "    SELECT \n" +
                "        UP_LOCATION,\n" +
                "        DIS_LOCATION,\n" +
                "        DIST,\n" +
                "        EX_DATE,\n" +
                "        ROW_NUMBER() OVER (PARTITION BY UP_LOCATION, DIS_LOCATION ORDER BY EX_DATE DESC) AS rn\n" +
                "    FROM SC_TB_TICKET_PRICE_ORDER\n" +
                "    WHERE DIST IS NOT NULL\n" +
                ") t\n" +
                "WHERE rn = 1";
        Table titket_price = tEnv.sqlQuery(TICKET_PRICE_ORDER);
        DataStream<Row> changelogStream  = tEnv.toChangelogStream(titket_price);

        // -------------------------- 核心：构建 UP_LOCATION_DIS_LOCATION -> DIST 的 Map --------------------------
        CloseableIterator<Row> ticketPriceIterator = null;
        List<Row> ticketPriceList = new ArrayList<>();
        try {
            ticketPriceIterator = changelogStream.executeAndCollect();
            // 手动遍历迭代器收集数据
            while (ticketPriceIterator.hasNext()) {
                ticketPriceList.add(ticketPriceIterator.next());
            }
            logger.info("成功收集机场对数据，共 {} 条记录", ticketPriceList.size());
        } catch (Exception e) {
            logger.error("收集机场对数据失败", e);
            throw e; // 抛出异常，中断任务
        } finally {
            // 强制关闭迭代器，释放资源
            if (ticketPriceIterator != null) {
                try {
                    ticketPriceIterator.close();
                } catch (Exception e) {
                    logger.warn("关闭迭代器失败", e);
                }
            }
        }
        // 构建 Map
        upDisToDistMap = new HashMap<>(ticketPriceList.size());
        for (Row row : ticketPriceList) {
            String upLocation = toStringSafe(row.getField("UP_LOCATION"));
            String disLocation = toStringSafe(row.getField("DIS_LOCATION"));
            String dist = toStringSafe(row.getField("DIST"));
            if (upLocation != null && disLocation != null) {
                StringBuilder keyBuilder = new StringBuilder(upLocation).append("_").append(disLocation);
                upDisToDistMap.put(keyBuilder.toString(), dist);
            }
        }
        logger.info("成功构建机场对-DIST映射，共 {} 条数据", upDisToDistMap.size());

        // 使用 RichMapFunction 安全初始化缓存
        SingleOutputStreamOperator<SegDimModel> mappedStream1 = rowDataStream
                .filter(row -> row != null && toStringSafe(row.getField("AIRLINE")) != null)
                .map(new RichMapFunction<Row, SegDimModel>() {
                    private transient FlightCache flightCache; // 关键：transient 避免序列化

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 每个 Task 初始化一次本地缓存
                        flightCache = new FlightCache();
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
                            return createEmptySegDimModel(segmentKey);
                        }
                        // 市场方航班号
                        String marketFlightNum = toStringSafe(row.getField("MARKET_FLIGHT_NUM"));
                        // 市场方航司
                        String marketAirline = toStringSafe(row.getField("MARKET_AIRLINE"));
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
                        // 按照航班号调用 API接口 获取数据
                        List<VariflightFlightModel> apiResult = VariflightRequest.fetchFlightsByFlightNo(
                                String.valueOf(departureDate), fullFlightNo
                        );
                        SegDimModel model = new SegDimModel();
                        // 查某个航班
                        VariflightFlightModel matchedFlight = null;
                        for(VariflightFlightModel flight : apiResult){
                            if(flight.getFlightDepcode().equals(depAirport) && flight.getFlightArrcode().equals(arrAirport)){
                                matchedFlight = flight;
                                break;
                            }
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
                            model.setIsCancelled(flightState != null && flightState.equals("取消") ? "是" : "否");
                        } else {
                            logger.debug("缓存中未找到航班: {}", fullFlightNo);
                            // 未找到航班，设置距离为0表示无效数据
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