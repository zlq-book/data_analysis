package com.travelsky.trp.usercenter.data.analysis.transform.zsf;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.VariflightFlightModel;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class VariflightRequest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    // 自定义 LocalDateTime 反序列化器
    static class LocalDateTimeDeserializer extends JsonDeserializer<LocalDateTime> {
        private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        @Override
        public LocalDateTime deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            String date = p.getText();
            // 处理空字符串的情况（取消的航班可能没有实际起降时间）
            if (date == null || date.trim().isEmpty()) {
                return null;
            }
            return LocalDateTime.parse(date, formatter);
        }
    }

    static {
        SimpleModule module = new SimpleModule();
        module.addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer());
        objectMapper.registerModule(module);
    }

    public static void main(String[] args) throws Exception {

        // 测试方式一：按日期+航班号查询
        System.out.println("=== 方式一测试 ===");
        List<VariflightFlightModel> flights1 = fetchFlightsByFlightNo("2021-01-18", "SC1182");
        for (VariflightFlightModel flight : flights1) {
            System.out.println(flight);
        }

        // 测试方式二：按日期+起飞机场+到达机场查询
        System.out.println("=== 方式二测试 ===");
        List<VariflightFlightModel> flights2 = fetchFlightsByDepArr("2021-01-18", "SZX", "TNA");
        for (VariflightFlightModel flight : flights2) {
            System.out.println(flight);
        }

        // 测试方式三：按日期+起飞城市+到达城市查询
        System.out.println("\n=== 方式三测试 ===");
        List<VariflightFlightModel> flights3 = fetchFlightsByCity("2014-09-18", "SHA", "BJS");
        for (VariflightFlightModel flight : flights3) {
            System.out.println(flight);
        }

        int pageNum = 1; // 起始页码（从1开始）
        // 开始时间
        long startTime = System.currentTimeMillis();
        while (true) {
            // 测试方式四：按日期+起飞机场+到达机场+起飞时间查询
            List<VariflightFlightModel> flights4 = fetchFlightsByAirportStatus("XMN", "2023-11-01", "DEP", pageNum, 100);
            if (flights4 == null || flights4.isEmpty()) {
                break;
            }
            for (VariflightFlightModel flight : flights4) {
                if(flight.getFlightNo().equals("SC2157")&&flight.getFlightArrcode().equals("JDZ"))
                    System.out.println(flight);
            }
            pageNum++;
        }
        // 结束时间
        long endTime = System.currentTimeMillis();
        System.out.println("总耗时：" + (endTime - startTime) + "ms");



    }

    /**
     * 分页查询并返回全部航班数据
     * @param airport 机场三字码（如 PEK）
     * @param date 日期（如 2025-11-13）
     * @param status 状态（如 DEP）
     * @return 全部航班数据的列表
     */
    public static List<VariflightFlightModel> fetchAllFlights(String airport, String date, String status) throws Exception {
        // 用于存储所有分页数据的总列表
        List<VariflightFlightModel> allFlights = new ArrayList<>();
        int pageNum = 1; // 起始页码（从1开始）
        int pageSize = 100; // 每页条数，可根据接口限制调整
        while (true) {
            // 调用分页接口查询当前页数据
            List<VariflightFlightModel> currentPageFlights = fetchFlightsByAirportStatus(
                    airport, date, status, pageNum, pageSize
            );
            // 如果当前页无数据，说明已查询完所有数据，退出循环
            if (currentPageFlights == null || currentPageFlights.isEmpty()) {
                break;
            }
            // 将当前页数据添加到总列表
            allFlights.addAll(currentPageFlights);
            // 页码+1，准备查询下一页
            pageNum++;
            // 可选：添加延迟，避免高频请求触发接口限流
//            try {
//                Thread.sleep(300); // 根据实际情况调整延迟时间（毫秒）
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt(); // 恢复中断状态
//                break; // 中断时退出循环
//            }
        }
        return allFlights;
    }

    /**
     * 方式一：按日期+航班号查询
     * @param date 日期 格式：yyyy-MM-dd
     * @param fnum 航班号（如：CA1111）
     */
    public static List<VariflightFlightModel> fetchFlightsByFlightNo(String date, String fnum) throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("appid", Constants.VARIFLIGHT_APP_ID);
        params.put("date", date);
        params.put("fnum", fnum);
        return fetchFlightData(params);
    }

    /**
     * 方式二：按日期+起飞机场+到达机场查询
     * @param date 日期 格式：yyyy-MM-dd
     * @param dep 起飞机场三字码
     * @param arr 到达机场三字码
     */
    public static List<VariflightFlightModel> fetchFlightsByDepArr(String date, String dep, String arr) throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("appid", Constants.VARIFLIGHT_APP_ID);
        params.put("date", date);
        params.put("dep", dep);
        params.put("arr", arr);
        return fetchFlightData(params);
    }

    /**
     * 方式三：按日期+起飞城市+到达城市查询
     * @param date 日期 格式：yyyy-MM-dd
     * @param depcity 起飞城市三字码
     * @param arrcity 到达城市三字码
     */
    public static List<VariflightFlightModel> fetchFlightsByCity(String date, String depcity, String arrcity) throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("appid", Constants.VARIFLIGHT_APP_ID);
        params.put("date", date);
        params.put("depcity", depcity);
        params.put("arrcity", arrcity);
        return fetchFlightData(params);
    }

    /**
     * 方式四：按机场+日期+状态查询（分页）
     * @param airport 机场三字码
     * @param date 日期 格式：yyyy-MM-dd
     * @param status 状态 DEP(出港) 或 ARR(进港)
     */
    public static List<VariflightFlightModel> fetchFlightsByAirportStatus(String airport, String date,
                                                                          String status,int page,int perpage) throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("appid", Constants.VARIFLIGHT_APP_ID);
        params.put("airport", airport);
        params.put("date", date);
        params.put("status", status);
        params.put("page",String.valueOf(page));
        params.put("perpage",String.valueOf(perpage));
        return fetchFlightData4(params);
    }

    private static List<VariflightFlightModel> fetchFlightData4(Map<String, String> params) throws Exception {
        // 步骤1：按 key 排序并拼接
        List<Map.Entry<String, String>> sortedEntries = new ArrayList<>(params.entrySet());
        sortedEntries.sort(Map.Entry.comparingByKey());

        StringBuilder strBuilder = new StringBuilder();
        for (int i = 0; i < sortedEntries.size(); i++) {
            Map.Entry<String, String> entry = sortedEntries.get(i);
            strBuilder.append(entry.getKey()).append("=").append(entry.getValue());
            if (i < sortedEntries.size() - 1) {
                strBuilder.append("&");
            }
        }
        String str = strBuilder.toString();

        // 步骤2：拼接 appsecurity 并进行双重 MD5 加密
        String concatStr = str + Constants.VARIFLIGHT_APPSECURITY;
        String token = md5(md5(concatStr));

        // 步骤3：构建完整 URL
        String fullUrl = Constants.VARIFLIGHT_BASE_URL + "?" + str + "&token=" + token;
        System.out.println("请求URL: " + fullUrl);

        // 步骤4：发送 GET 请求
        URL url = new URL(fullUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(10000);
        conn.setReadTimeout(10000);

        int responseCode = conn.getResponseCode();
        if (responseCode != 200) {
            throw new RuntimeException("HTTP Error: " + responseCode);
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
        StringBuilder response = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            response.append(line);
        }
        reader.close();
        conn.disconnect();

//        System.out.println("响应数据: " + response.toString());

        JsonNode root = objectMapper.readTree(String.valueOf(response));
        JsonNode data = root.get("data");

        // 解析为 List
        return objectMapper.readValue(data.traverse(), new TypeReference<List<VariflightFlightModel>>() {});
    }

    /**
     * 通用查询方法
     */
    private static List<VariflightFlightModel> fetchFlightData(Map<String, String> params) throws Exception {
        // 步骤1：按 key 排序并拼接
        List<Map.Entry<String, String>> sortedEntries = new ArrayList<>(params.entrySet());
        sortedEntries.sort(Map.Entry.comparingByKey());

        StringBuilder strBuilder = new StringBuilder();
        for (int i = 0; i < sortedEntries.size(); i++) {
            Map.Entry<String, String> entry = sortedEntries.get(i);
            strBuilder.append(entry.getKey()).append("=").append(entry.getValue());
            if (i < sortedEntries.size() - 1) {
                strBuilder.append("&");
            }
        }
        String str = strBuilder.toString();

        // 步骤2：拼接 appsecurity 并进行双重 MD5 加密
        String concatStr = str + Constants.VARIFLIGHT_APPSECURITY;
        String token = md5(md5(concatStr));

        // 步骤3：构建完整 URL
        String fullUrl = Constants.VARIFLIGHT_BASE_URL + "?" + str + "&token=" + token;
        System.out.println("请求URL: " + fullUrl);

        // 步骤4：发送 GET 请求
        URL url = new URL(fullUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(10000);
        conn.setReadTimeout(10000);

        int responseCode = conn.getResponseCode();
        if (responseCode != 200) {
            throw new RuntimeException("HTTP Error: " + responseCode);
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
        StringBuilder response = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            response.append(line);
        }
        reader.close();
        conn.disconnect();

        System.out.println("响应数据: " + response.toString());

        // 解析为 List
        return objectMapper.readValue(response.toString(), new TypeReference<List<VariflightFlightModel>>() {});
    }

    // MD5 加密（返回 32 位小写）
    private static String md5(String input) throws Exception {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
        return bytesToHex(digest);
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }
}