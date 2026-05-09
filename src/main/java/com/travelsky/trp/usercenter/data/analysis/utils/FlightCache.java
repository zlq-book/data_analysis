package com.travelsky.trp.usercenter.data.analysis.utils;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.VariflightFlightModel;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FlightCache {
    private final ConcurrentHashMap<String, Map<String, VariflightFlightModel>> cache = new ConcurrentHashMap<>();
    private final long ttlMillis = 24 * 60 * 60 * 1000; // 24小时过期


//    public Cache<String,Map<String, VariflightFlightModel>> local_Cache(){
//        return Caffeine.newBuilder()
//                .initialCapacity(1024) // 初始容量
//                .maximumSize(10_000)
//                .expireAfterWrite(Duration.ofMinutes(5)) // 写入后5分钟过期
//                .build();
//    }

//    private final Cache<String, Map<String, VariflightFlightModel>> caffeineCache = local_Cache();

    private final ObjectMapper objectMapper = new ObjectMapper();

    // 缓存 key: dep+arr+date
    private String getKey(String dep, String status, LocalDate date) {
        return dep + "_" + status + "_" + date.toString();
    }

    // 获取缓存中的航班列表
    public List<VariflightFlightModel> getFlights(String dep, String arr, LocalDate date) {
        String key = getKey(dep, arr, date);
        Map<String, VariflightFlightModel> flightMap = cache.get(key);
//        Map<String, VariflightFlightModel> flightMap = caffeineCache.getIfPresent(key);

        if (flightMap != null && !isExpired(key)) {
            return new ArrayList<>(flightMap.values());
        }
        return null;
    }

    // 根据航班号查单个航班
    public VariflightFlightModel getFlightByNo(String flightNo, String dep, String arr, LocalDate date) throws IOException {
        String key = getKey(dep, arr, date);
        Map<String, VariflightFlightModel> flightMap = cache.get(key);
//        Map<String, VariflightFlightModel> flightMap = caffeineCache.getIfPresent(key);
        if (flightMap != null && !isExpired(key)) {
            return flightMap.get(flightNo);
        }
        return null;
    }

    // 写入缓存（带时间戳）
    public void putFlights(String dep, String arr, LocalDate date, List<VariflightFlightModel> flights) {
        String key = getKey(dep, arr, date);
        Map<String, VariflightFlightModel> flightMap = new HashMap<>();

        for (VariflightFlightModel flight : flights) {
//            flightMap.put(flight.getFlightNo()+"_"+flight.getFlightDepcode(), flight);
            flightMap.put(flight.getFlightNo(), flight);
        }
        // 存入缓存，并记录时间
        cache.put(key, flightMap);
//        caffeineCache.put(key, flightMap);
    }

    // 检查是否过期
    private boolean isExpired(String key) {
        // 可以扩展为存储时间戳，这里简化处理：只判断是否存在且未超时
        // 实际生产建议用 Guava Cache 或 Caffeine
        return false; // 示例中不实现，仅示意
    }

    /**
     * 反序列化为原方法的返回类型
     */
    private Object deserialize(String json) throws IOException {
        if ("null".equals(json)) {
            return null;
        }
        // 使用 Jackson 反序列化泛型
        JavaType javaType = objectMapper.getTypeFactory().constructType(VariflightFlightModel.class);
        return objectMapper.readValue(json, javaType);
    }
}