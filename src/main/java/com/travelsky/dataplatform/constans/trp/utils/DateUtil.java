package com.travelsky.dataplatform.constans.trp.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;

/**
 * @program: main_release
 * @description: 日期工具类
 * @author: Wanghy
 * @create: 2020-10-01 19:56
 **/
public class DateUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DateUtil.class);
    public static final String PATTERN_YYYYMMDD = "yyyyMMdd";
    public static final String DATE_PATTERN = "yyyy-MM-dd";
    private static final String YEAR_PATTERN = "yyyy";
    private static final String MONTH_PATTERN = "yyyy-MM";
    public static final String NO_SECOND_DATETIME_PATTERN = "yyyy-MM-dd HH:mm";
    public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DATETIME_PATTERN = "yyyy-MM-dd'T'HH:mm";
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    private static final DateTimeFormatter TRADITIONAL_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 时间格式化，格式化失败返回空字符
     * @param date
     * @param pattern
     * @return
     */
    public static String formatDate(Date date, String pattern) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(pattern);
            return sdf.format(date);
        } catch (Exception e) {
            return "";
        }

    }
    /**
     * 将时间字符串转换成HHmmss格式Date对象
     * @param strDate
     * @return
     */
    public static Date parse(String strDate) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("HHmmss");
        return sdf.parse(strDate);
    }
    /**
     * 将时间字符串转换成指定格式Date对象
     * @param strDate
     * @return
     */
    public static Date parse(String strDate, String pattern) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.parse(strDate);
    }
    // ISO → 传统格式
    public static String isoToTraditional(String isoDateTime) {
        LocalDateTime localDateTime = LocalDateTime.parse(isoDateTime, ISO_FORMATTER);
        return localDateTime.format(TRADITIONAL_FORMATTER);
    }
    /**
     * 从 ISO 格式中提取日期部分 (yyyy-MM-dd)
     * @param isoDateTime ISO 格式的日期时间字符串，如 "2025-08-26T10:30:45"
     * @return 日期字符串，格式为 yyyy-MM-dd
     */
    public static String extractDateFromIso(String isoDateTime) {
        if (StringUtils.isBlank(isoDateTime)) {
            return null;
        }
        LocalDateTime dateTime = LocalDateTime.parse(isoDateTime);
        return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    }

    /**
     * 从 ISO 格式中提取时间部分 (HH:mm:ss)
     * @param isoDateTime ISO 格式的日期时间字符串，如 "2025-08-26T10:30:45"
     * @return 时间字符串，格式为 HH:mm:ss
     */
    public static String extractTimeFromIso(String isoDateTime) {
        if (StringUtils.isBlank(isoDateTime)) {
            return null;
        }
        LocalDateTime dateTime = LocalDateTime.parse(isoDateTime);
        return dateTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
    }
    /**
     * 将long类型时间根据指定类型格式化
     * @param timeStamp
     * @param pattern
     * @return
     */
    public static String formatDate(long timeStamp, String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        String sd = sdf.format(new Date(timeStamp));
        return sd;
    }

    /**
     * 将时间转化成long时间值
     * @param s
     * @param pattern
     * @return
     * @throws ParseException
     */
    public static Long dateToTimestamp(String s, String pattern) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        Date date = sdf.parse(s);
        Long timetimestamp = date.getTime();
        return timetimestamp;
    }

    /**
     * 将日期数据格式转换成没有分隔符的格式
     *
     * @param s
     * @return
     */
    public static String dateFormatChange(String s) {
        if (s.length() == 8) {
            s = s.replace("-", "");
        } else {
            s = s.replace("-", "").replace(":", "").replace(" ","").split("\\.")[0];
        }
        return s;
    }

    /**
     * 根据日期范围，获取按周期划分的日期区间
     *
     * @param startDate 开始日期
     * @param endDate   结束日期
     * @param pattern   日期格式（支持：DATE_PATTERN，MONTH_PATTERN，YEAR_PATTERN）
     * @return List<String> 区间集合 例如：[2020-11-29,2020-11-30,2020-12-01,2020-12-02]
     */
    public static List<String> getDateStrList(Date startDate, Date endDate, String pattern) {
        List<String> result = new ArrayList<>();

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(startDate);
        if (DATE_PATTERN.equals(pattern)) {
            while (startDate.before(endDate) || startDate.equals(endDate)) {
                result.add(new SimpleDateFormat(DATE_PATTERN).format(calendar.getTimeInMillis()));
                calendar.add(Calendar.DATE, 1);
                startDate = calendar.getTime();
            }
        } else if (MONTH_PATTERN.equals(pattern)) {
            while (startDate.before(endDate) || startDate.equals(endDate)) {
                result.add(new SimpleDateFormat(MONTH_PATTERN).format(calendar.getTimeInMillis()));
                calendar.add(Calendar.MONTH, 1);
                startDate = calendar.getTime();
            }
        } else if (YEAR_PATTERN.equals(pattern)) {
            while (startDate.before(endDate) || startDate.equals(endDate)) {
                result.add(new SimpleDateFormat(YEAR_PATTERN).format(calendar.getTimeInMillis()));
                calendar.add(Calendar.YEAR, 1);
                startDate = calendar.getTime();
            }
        }
        return result;
    }

    /**
     * 根据日期范围，获取按周期划分的日期区间
     *
     * @param startDateStr 开始日期（格式：2020-11-29）
     * @param endDateStr   结束日期（格式：2020-12-02）
     * @param pattern      日期格式（支持：DATE_PATTERN，MONTH_PATTERN，YEAR_PATTERN）
     * @return List<String> 区间集合 例如：[2020-11-29,2020-11-30,2020-12-01,2020-12-02]
     */
    public static List<String> getDateStrList(String startDateStr, String endDateStr, String pattern) {
        Date start = dateParse(startDateStr, pattern);
        Date end = dateParse(endDateStr, pattern);
        return getDateStrList(start, end, pattern);
    }

    /**
     * 日期字符串转换为日期(java.util.Date)
     *
     * @param dateStr 日期字符串
     * @param pattern 日期格式 例如DATETIME_PATTERN
     * @return Date 日期
     */
    public static Date dateParse(String dateStr, String pattern) {
        Date date = null;
        SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
        // 设置lenient为false. 否则SimpleDateFormat会比较宽松地验证日期，比如2007/02/29会被接受，并转换成2007/03/01
        dateFormat.setLenient(false);
        try {
            date = dateFormat.parse(dateStr);
        } catch (ParseException e) {
            LOG.error(e.getMessage(),e);
        }
        return date;
    }

    /**
     * 得到UTC时间，类型为字符串，格式为"yyyy-MM-dd HH:mm"
     * 如果获取失败，返回null
     *
     * @return
     */
    public static String getUTCTimeStr() {
        StringBuffer UTCTimeBuffer = new StringBuffer();
        // 1、取得本地时间：
        Calendar cal = Calendar.getInstance();
        // 2、取得时间偏移量：
        int zoneOffset = cal.get(Calendar.ZONE_OFFSET);
        // 3、取得夏令时差：
        int dstOffset = cal.get(Calendar.DST_OFFSET);
        // 4、从本地时间里扣除这些差量，即可以取得UTC时间：
        cal.add(Calendar.MILLISECOND, -(zoneOffset + dstOffset));
        int year = cal.get(Calendar.YEAR);
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DAY_OF_MONTH);
        int hour = cal.get(Calendar.HOUR_OF_DAY);
        int minute = cal.get(Calendar.MINUTE);
        UTCTimeBuffer.append(year).append("-").append(month).append("-").append(day);
        UTCTimeBuffer.append(" ").append(hour).append(":").append(minute);
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(NO_SECOND_DATETIME_PATTERN);
            sdf.parse(UTCTimeBuffer.toString());
            return UTCTimeBuffer.toString();
        } catch (ParseException e) {
            LOG.error(e.getMessage(),e);
        }
        return null;
    }

    /**
     * 比较传进来的日期是否大于当前日期，如果传进来的日期大于当前日期则返回true，否则返回false
     *
     * @param dateStr 日期字符串
     * @param pattern 日期格式
     * @return boolean
     */
    public static boolean compareNowDate(String dateStr, String pattern) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        try {
            Date date = simpleDateFormat.parse(dateStr);
            return date.after(new Date());
        } catch (ParseException e) {
            LOG.error(e.getMessage(),e);
        }
        return false;
    }


    /**
     * 根据日历返回日期时间字符串
     *
     * @param calendar 日历
     * @return String 日期时间字符串
     */
    public static String getDateTimeStr(Calendar calendar) {
        StringBuffer buf = new StringBuffer("");

        buf.append(calendar.get(Calendar.YEAR));
        buf.append("-");
        buf.append(calendar.get(Calendar.MONTH) + 1 > 9 ? calendar.get(Calendar.MONTH) + 1 + ""
                : "0" + (calendar.get(Calendar.MONTH) + 1));
        buf.append("-");
        buf.append(calendar.get(Calendar.DAY_OF_MONTH) > 9 ? calendar.get(Calendar.DAY_OF_MONTH) + ""
                : "0" + calendar.get(Calendar.DAY_OF_MONTH));
        buf.append(" ");
        buf.append(calendar.get(Calendar.HOUR_OF_DAY) > 9 ? calendar.get(Calendar.HOUR_OF_DAY) + ""
                : "0" + calendar.get(Calendar.HOUR_OF_DAY));
        buf.append(":");
        buf.append(calendar.get(Calendar.MINUTE) > 9 ? calendar.get(Calendar.MINUTE) + ""
                : "0" + calendar.get(Calendar.MINUTE));
        buf.append(":");
        buf.append(calendar.get(Calendar.SECOND) > 9 ? calendar.get(Calendar.SECOND) + ""
                : "0" + calendar.get(Calendar.SECOND));
        return buf.toString();
    }

    /**
     * 获取指定日期前后num天的集合，带日期格式参数
     *
     * @param date    指定日期
     * @param num     天数（正数：之后；负数：之前）
     * @param pattern 日期格式
     * @return List<String> 前/后日期的集合（包含指定日期）  例如：[2020-11-29,2020-11-30,2020-12-01]
     */
    public static List<String> getDateStrList(Date date, int num, String pattern) {
        List<String> result = new ArrayList<>();
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        boolean flag = false;
        if (num < 0) {
            num = Math.abs(num);
            flag = true;
        }
        for (int i = 0; i < num; i++) {
            result.add(new SimpleDateFormat(pattern).format(c.getTimeInMillis()));
            c.add(Calendar.DATE, flag ? -1 : 1);
        }
        if (flag) {
            Collections.reverse(result);
        }
        return result;
    }

    /**
     * 获取指定日期前后num天的日期，带日期格式参数
     *
     * @param date    指定日期
     * @param num     天数（正数：之后；负数：之前）
     * @param pattern 日期格式
     * @return List<String> 前/后日期的集合（包含指定日期）  例如：[2020-11-29,2020-11-30,2020-12-01]
     */
    public static String getDateStr(Date date, int num, String pattern) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        boolean flag = false;
        if (num < 0) {
            num = Math.abs(num);
            flag = true;
        }
        c.add(Calendar.DATE, flag ? -num : num);

        return new SimpleDateFormat(pattern).format(c.getTimeInMillis());
    }


    /**
     * 获取本月的第一天
     *
     * @return Calendar 日历
     */
    public static Calendar getStartDayOfMonth(Date date) {
        Calendar calendar = Calendar.getInstance(Locale.CHINA);
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        return calendar;
    }

    /**
     * 获取本月的最后一天
     *
     * @return Calendar 日历
     */
    public static Calendar getEndDayOfMonth(Date date) {
        Calendar calendar = Calendar.getInstance(Locale.CHINA);
        calendar.setTime(date);
        int i = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
        calendar.set(Calendar.DAY_OF_MONTH, i);
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);
        return calendar;
    }


    /**
     * 比较日期
     *
     * @return boolean
     */
    public static boolean compareDateBetween(String beginDateStr, String endDateStr, String compareDateStr) {
        String formats = "yyyyMMdd";

        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(formats, Locale.CHINA);
            Date beginDate = simpleDateFormat.parse(beginDateStr);
            Date endDate = simpleDateFormat.parse(endDateStr);
            Date compareDate = simpleDateFormat.parse(compareDateStr);

            if ((compareDate.after(beginDate)) && (compareDate.before(endDate))) {
                return true;
            }
            if (beginDate.getTime() == compareDate.getTime()) {
                return true;
            }
            if (endDate.getTime() == compareDate.getTime()) {
                return true;
            }
        } catch (ParseException e) {
            return false;
        }
        return false;

    }

    /**
     *
     * @param dateStr
     * @param days
     * @return
     */
    public static String getDateAddDayInterval(String dateStr, int days) {
        String formats = "yyyyMMdd";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(formats, Locale.CHINA);
        try {
            Date inputDate = simpleDateFormat.parse(dateStr);
            Calendar calendar = Calendar.getInstance(Locale.CHINA);
            calendar.setTime(inputDate);
            calendar.add(Calendar.DATE, days);
            return simpleDateFormat.format(calendar.getTime());

        } catch (ParseException e) {
            LOG.error(e.getMessage(),e);
        }
        return "";
    }

    /**
     *对比时间
     * @param comparedStr
     * @param compareStr
     * @return
     */
    public static int compareDate(String comparedStr, String compareStr) {
        try {
            String formats = "yyyyMMdd";

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(formats, Locale.CHINA);
            Date comparedDate = simpleDateFormat.parse(comparedStr);
            Date compareDate = simpleDateFormat.parse(compareStr);
            if (comparedDate.getTime() == compareDate.getTime()) {
                return 0;
            }
            if (compareDate.after(comparedDate)) {
                return 1;
            } else {
                return -1;
            }

        } catch (ParseException e) {
        }
        return -1;
    }

    /**
     * @param startDay 起始日期
     * @param endDay   结束日期
     * @param pattern  日期格式
     * @return
     * @throws ParseException
     */
    public static int daysBetween(String startDay, String endDay, String pattern) throws ParseException {
        DateFormat dft = new SimpleDateFormat(pattern);
        Date star = dft.parse(startDay);
        Date end = dft.parse(endDay);
        Long starTime = star.getTime();
        Long endTime = end.getTime();
        //时间戳相差的毫秒数
        Long num = endTime - starTime;
        //除以一天的毫秒数
        return (int) (num / 24 / 60 / 60 / 1000);

    }
    /**
     * 将Date对象格式化为yyyyMMdd格式字符串
     * @param date 待格式化的日期对象
     * @return 格式化后的字符串（如20240818），若date为null则返回null
     */
    public static String formatToYmd(Date date) {
        if (date == null) {
            return null;
        }
        return formatDate(date, "yyyyMMdd");
    }

    /**
     * 将指定格式的日期字符串转换为yyyyMMdd格式
     * @param dateStr 原始日期字符串
     * @return 格式化后的yyyyMMdd字符串，转换失败返回null
     */
    public static String formatToYmd(String dateStr) {
        if (dateStr == null) {
            return null;
        }
        try {
            Date date = parse(dateStr, DATETIME_PATTERN);
            return formatDate(date, "yyyyMMdd");
        } catch (ParseException e) {
            LOG.error("Failed to format date string: {} with pattern: {}", dateStr, e);
            return null;
        }
    }
    /**
     * 将yyyyMMdd格式的字符串转换为yyyy-MM-dd格式
     * @param dateStr 8位数字日期字符串
     * @return 格式化后的日期字符串，如果输入无效则返回null
     */
    public static String convertToStandardFormat(String dateStr) {
        return convertFormat(dateStr, PATTERN_YYYYMMDD, DATE_PATTERN);
    }

    /**
     * 日期格式转换
     * @param dateStr 日期字符串
     * @param inputPattern 输入格式
     * @param outputPattern 输出格式
     * @return 格式化后的日期字符串，如果输入无效则返回null
     */
    public static String convertFormat(String dateStr, String inputPattern, String outputPattern) {
        if (dateStr == null || dateStr.trim().isEmpty()) {
            return null;
        }

        try {
            DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern(inputPattern);
            DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern(outputPattern);

            LocalDate date = LocalDate.parse(dateStr, inputFormatter);
            return date.format(outputFormatter);
        } catch (DateTimeParseException e) {
            return null;
        }
    }
}
