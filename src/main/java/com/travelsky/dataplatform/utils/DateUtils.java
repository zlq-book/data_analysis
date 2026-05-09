package com.travelsky.dataplatform.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class DateUtils {

    // 定义输入和输出的日期时间格式
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 将日期字符串转换为当天的开始时间和结束时间
     * @param dateString 格式为 "yyyy-MM-dd" 的字符串
     * @return 包含开始和结束时间的 DayTimeRange 对象
     */
    public static DayTimeRange getDayTimeRange(String dateString) {
        // 1. 将输入字符串解析为 LocalDate 对象
        LocalDate date = LocalDate.parse(dateString, DATE_FORMATTER);

        // 2. 获取当天的开始时间 (00:00:00)
        LocalDateTime startDateTime = date.atStartOfDay(); // 这是获取一天开始最便捷的方法

        // 3. 获取当天的结束时间 (23:59:59)
        //    a. 获取一天的最大时间 (23:59:59.999999999)
        //    b. 然后将其截断到秒，得到 23:59:59
        LocalDateTime endDateTime = date.atTime(LocalTime.MAX).withNano(0);

        // 4. 将 LocalDateTime 对象格式化为目标字符串
        String startTimeStr = startDateTime.format(DATE_TIME_FORMATTER);
        String endTimeStr = endDateTime.format(DATE_TIME_FORMATTER);

        // 5. 封装并返回结果
        return new DayTimeRange(startTimeStr, endTimeStr);
    }

    public static void main(String[] args) {
        String inputDate = "2022-01-01";
        DayTimeRange range = getDayTimeRange(inputDate);

        System.out.println("输入日期: " + inputDate);
        System.out.println("开始时间: " + range.getStartTime()); // 输出: 2022-01-01 00:00:00
        System.out.println("结束时间: " + range.getEndTime());   // 输出: 2022-01-01 23:59:59

        // 测试闰年
        String leapYearDate = "2024-02-29";
        DayTimeRange leapYearRange = getDayTimeRange(leapYearDate);
        System.out.println("\n输入日期: " + leapYearDate);
        System.out.println("开始时间: " + leapYearRange.getStartTime());
        System.out.println("结束时间: " + leapYearRange.getEndTime());
    }
}
