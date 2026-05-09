package com.travelsky.trp.usercenter.data.analysis.transform.zsf;

import com.travelsky.trp.usercenter.data.analysis.model.dim.SegDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.VariflightFlightModel;
import com.travelsky.trp.usercenter.data.analysis.utils.FlightCache;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

public class AsyncFlightFetchFunction extends RichAsyncFunction<Row, SegDimModel> {

    private static final Logger logger = LoggerFactory.getLogger(AsyncFlightFetchFunction.class);

    private transient ExecutorService executorService;
    private transient FlightCache flightCache;
    private transient Map<String, String> upDisToDistMap;

    // 用于控制接口调用的并发（避免同时调用太多次相同的接口）
    private transient ConcurrentHashMap<String, CompletableFuture<List<VariflightFlightModel>>> loadingCache;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化线程池
        this.executorService = Executors.newFixedThreadPool(10);
        this.flightCache = new FlightCache();
        this.loadingCache = new ConcurrentHashMap<>();
        this.upDisToDistMap = VariflightToSegDimTrans2.getUpDisToDistMap();
        logger.info("AsyncFlightFetchFunction initialized with thread pool size: 10");
    }

    @Override
    public void close() throws Exception {
        if (executorService != null) {
            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        }
        logger.info("AsyncFlightFetchFunction closed");
    }

    @Override
    public void asyncInvoke(Row row, ResultFuture<SegDimModel> resultFuture) {

        CompletableFuture.supplyAsync(() -> {
            try {
                // 1. 提取基础字段
                String segmentKey = toStringSafe(row.getField("SEGMENT_KEY"));
                String airline = toStringSafe(row.getField("AIRLINE"));
                String departureDateStr = toStringSafe(row.getField("DEPARTURE_DATE"));
                String marketFlightNum = toStringSafe(row.getField("MARKET_FLIGHT_NUM"));
                String marketAirline = toStringSafe(row.getField("MARKET_AIRLINE"));

                logger.debug("Processing segment: {}, airline: {}, date: {}", segmentKey, airline, departureDateStr);

                // 2. 解析机场代码
                String depAirport;
                String arrAirport = "XMN";
                if (airline != null && airline.contains("_")) {
                    String[] parts = airline.split("_", 2);
                    if (parts.length == 2) {
                        depAirport = parts[0];
                        arrAirport = parts[1];
                    } else {
                        depAirport = "CTU";
                    }
                } else {
                    depAirport = "CTU";
                }

                LocalDate departureDate = LocalDate.parse(departureDateStr);
                String fullFlightNo = marketAirline + marketFlightNum;
                String cacheKey = depAirport + "_DEP_" + departureDateStr;

                // 3. 从缓存获取或异步加载航班数据
                List<VariflightFlightModel> flightList = flightCache.getFlights(depAirport, "DEP", departureDate);

                if (flightList == null) {
                    // 使用 loadingCache 避免重复调用相同的接口
                    CompletableFuture<List<VariflightFlightModel>> loadingFuture = loadingCache.computeIfAbsent(
                            cacheKey,
                            key -> CompletableFuture.supplyAsync(() -> {
                                try {
                                    logger.info("Fetching flights from API for: {}", key);
                                    List<VariflightFlightModel> flights = VariflightRequest.fetchAllFlights(
                                            depAirport, departureDateStr, "DEP"
                                    );
                                    flightCache.putFlights(depAirport, "DEP", departureDate, flights);
                                    logger.info("Successfully cached {} flights for: {}",
                                            flights != null ? flights.size() : 0, key);
                                    return flights;
                                } catch (Exception e) {
                                    logger.error("Failed to fetch flights for: " + key, e);
                                    return Collections.emptyList();
                                } finally {
                                    // 请求完成后从 loadingCache 中移除
                                    loadingCache.remove(key);
                                }
                            }, executorService)
                    );

                    // 等待加载完成（带超时）
                    flightList = loadingFuture.get(25, TimeUnit.SECONDS);
                }

                // 4. 匹配航班
                VariflightFlightModel matchedFlight = null;
                if (flightList != null && !fullFlightNo.isEmpty()) {
                    String finalArrAirport = arrAirport;
                    matchedFlight = flightList.stream()
                            .filter(f -> (f.getFlightNo() + "_" + f.getFlightArrAirport())
                                    .equals(fullFlightNo + "_" + finalArrAirport))
                            .findFirst()
                            .orElse(null);

                    if (matchedFlight != null) {
                        logger.debug("Matched flight: {} for segment: {}", matchedFlight.getFlightNo(), segmentKey);
                    } else {
                        logger.debug("No matching flight found for: {} -> {}", fullFlightNo, arrAirport);
                    }
                }

                // 5. 构建结果模型
                SegDimModel model = new SegDimModel();
                model.setSegmentKey(segmentKey);

                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
                model.setUpdateTime(LocalDateTime.now().format(formatter));

                if (matchedFlight != null) {
                    // 提取出航班的推出时间和推入时间
                    LocalDateTime flightOutgateTime = matchedFlight.getFlightOutgateTime();
                    LocalDateTime flightIngateTime = matchedFlight.getFlightIngateTime();
                    String departureDateString = toStringSafe(row.getField("DEPARTURE_DATE"));
                    String departureTimeString = toStringSafe(row.getField("DEPARTURE_TIME"));
                    String arrivalDateString = toStringSafe(row.getField("ARRIVAL_DATE"));
                    String arrivalTimeString = toStringSafe(row.getField("ARRIVAL_TIME"));
                    LocalDateTime plannedDepartureTime = null;
                    LocalDateTime plannedArrivalTime = null;
                    // 只在日期和时间都存在的情况下进行解析
                    if (departureDateString != null && departureTimeString != null) {
                        plannedDepartureTime = LocalDateTime.parse(departureDateString + "T" + departureTimeString);
                    } else {
                        logger.warn("Missing departure date or time for segment {}", segmentKey);
                    }
                    if (arrivalDateString != null && arrivalTimeString != null) {
                        plannedArrivalTime = LocalDateTime.parse(arrivalDateString + "T" + arrivalTimeString);
                    } else {
                        logger.warn("Missing arrival date or time for segment {}", segmentKey);
                    }

                    // 初始化状态为正常
                    String delayStatus = "正常"; // ZHCH
                    String delayCode = "ZHCH";
                    // 判断出港延误
                    if (flightOutgateTime != null && plannedDepartureTime != null) {
                        long departureDelay = java.time.Duration.between(plannedDepartureTime, flightOutgateTime).toMinutes();
                        if (departureDelay >= 15) {
                            delayStatus = "出港延误"; // CGYW
                            delayCode = "CGYW";
                        }
                    }

                    // 判断进港延误
                    if (flightIngateTime != null && plannedArrivalTime != null) {
                        long arrivalDelay = java.time.Duration.between(plannedArrivalTime, flightIngateTime).toMinutes();
                        if (arrivalDelay >= 15) {
                            // 确保如果同时有出港和进港延误，只记录出港延误
                            if (!delayStatus.equals("出港延误")) {
                                delayStatus = "进港延误"; // JGYW
                                delayCode = "JGYW";
                            }
                        }
                    }
                    // 飞机是否延误
                    model.setIsDelayed(delayCode);
                    // 飞机型号
                    model.setAirtype(matchedFlight.getGeneric());
                    model.setDistanceTpm(upDisToDistMap.get(airline));
                    // 航班是否取消
                    model.setIsCancelled(matchedFlight.getFlightState().equals("取消")?"是":"否");
                }

                // 设置距离
                String distKey = depAirport + "_" + arrAirport;
                if (upDisToDistMap != null) {
                    model.setDistanceTpm(upDisToDistMap.get(distKey));
                }
                logger.debug("Successfully processed segment: {}", segmentKey);
                return model;

            } catch (TimeoutException e) {
                logger.error("Timeout while fetching flight data for row: " + row, e);
                return null;
            } catch (Exception e) {
                logger.error("Error processing row: " + row, e);
                return null;
            }
        }, executorService).whenComplete((result, throwable) -> {
            if (throwable != null) {
                logger.error("Async task failed", throwable);
                resultFuture.complete(Collections.emptyList());
            } else if (result != null) {
                resultFuture.complete(Collections.singletonList(result));
            } else {
                resultFuture.complete(Collections.emptyList());
            }
        });
    }

    private static String toStringSafe(Object obj) {
        return obj != null ? obj.toString() : null;
    }
}
