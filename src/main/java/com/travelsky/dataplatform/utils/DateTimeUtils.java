package com.travelsky.dataplatform.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.text.DateFormatter;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * @author kuangaihua
 * @date 2025/8/5 11:17
 */
public class DateTimeUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DateTimeUtils.class);
    public static String getCurrentDateTime()
    {
        // 获取当前时间
        LocalDateTime now = LocalDateTime.now();
        // 定义日期时间格式（可自定义，如 "yyyy-MM-dd HH:mm:ss.SSS"）
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        // 格式化为字符串
        String formattedDateTime = now.format(formatter);
        return formattedDateTime;
    }
    public static String localDateToString(LocalDate date)
    {
        if (date == null){
            return null;
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String dateStr = date.format(formatter);
        return dateStr;
    }
    public static String localDateTimeToString(LocalDateTime dateTime)
    {
        if (dateTime == null){
            return null;
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        String dateTimeStr = dateTime.format(formatter);
        return dateTimeStr;
    }
    public static String localDateTimeToDateString(LocalDateTime dateTime)
    {
        if (dateTime == null){
            return null;
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String dateTimeStr = dateTime.format(formatter);
        return dateTimeStr;
    }
    public static String localDateTimeToTimeString(LocalDateTime dateTime)
    {
        if (dateTime == null){
            return null;
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
        String dateTimeStr = dateTime.format(formatter);
        return dateTimeStr;
    }
    /**
     *  格式化日期时间字符串
     *  @param sourceDateTime 源日期时间字符串
     *  @param format 源日期时间字符串的格式
     *
     *  @return 格式化后的日期时间字符串
     * 示例：2025/08/05
     * 输出：2025-08-05 00:00:00.000
     * */
    public static String dateFormatToDateTime(String sourceDateTime,String format)
    {
        if (sourceDateTime == null || sourceDateTime.isEmpty()) {
            return null;
        }
        DateTimeFormatter sourceFormatter = DateTimeFormatter.ofPattern(format);
        LocalDateTime dateTime = LocalDate.parse(sourceDateTime, sourceFormatter).atStartOfDay();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        String dateTimeStr = dateTime.format(formatter);
        return dateTimeStr;
    }

    /**
     *  格式化日期时间字符串
     *  @param sourceDateTime 源日期时间字符串 带T 2025-09-28T11:35:31
     *  @param format 源日期时间字符串的格式
     *
     *  @return 格式化后的日期时间字符串 format
     * 示例：2025/08/05
     * 输出： format
     * */
    public static String dateTimeParseToDateFormat(String sourceDateTime,String format)
    {
        if (sourceDateTime == null || sourceDateTime.isEmpty()) {
            return null;
        }
        LocalDateTime dateTime = LocalDateTime.parse(sourceDateTime);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
        String dateTimeStr = dateTime.format(formatter);
        return dateTimeStr;
    }

    /**
     * 解析ISO格式的日期时间字符串，返回格式化的日期和时间
     *
     * @param dateTimeStr ISO格式的日期时间字符串，如 "2022-04-03T07:30:12"
     * @return 包含formattedDate和formattedTime的Map
     */
    public static Map<String, String> parseAndFormatDateTime(String dateTimeStr) {
        Map<String, String> result = new HashMap<>();
        result.put("formattedDate", null);
        result.put("formattedTime", null);

        if (dateTimeStr != null && !dateTimeStr.isEmpty()) {
            try {
                LocalDateTime dateTime = LocalDateTime.parse(dateTimeStr);
                DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

                result.put("formattedDate", dateTime.format(dateFormatter));
                result.put("formattedTime", dateTime.format(timeFormatter));
            } catch (Exception e) {
                LOG.warn("日期时间格式解析错误: {}", dateTimeStr);
            }
        }

        return result;
    }

    /**
     * 解析ISO格式的日期时间字符串，返回格式化的日期
     *
     * @param dateTimeStr ISO格式的日期时间字符串
     * @return 格式化后的日期字符串
     */
    public static String parseAndFormatDate(String dateTimeStr) {
        return parseAndFormatDateTime(dateTimeStr).get("formattedDate");
    }

    /**
     * 解析ISO格式的日期时间字符串，返回格式化的时间
     *
     * @param dateTimeStr ISO格式的日期时间字符串
     * @return 格式化后的时间字符串
     */
    public static String parseAndFormatTime(String dateTimeStr) {
        return parseAndFormatDateTime(dateTimeStr).get("formattedTime");
    }

}

