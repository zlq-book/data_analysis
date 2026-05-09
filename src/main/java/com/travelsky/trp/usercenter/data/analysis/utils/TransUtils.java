package com.travelsky.trp.usercenter.data.analysis.utils;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimModel;
import dm.jdbc.util.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/20  17:35
 */
public class TransUtils {

    static final Logger logger = LoggerFactory.getLogger(TransUtils.class);
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    public static final DateTimeFormatter DATE_FORMATTER_TWO = DateTimeFormatter.ofPattern("yyyyMMdd");
    public static final Pattern KEY_ACCOUNT_PATTERN = Pattern.compile("^([0-9]{8}|[0-9a-zA-Z]{5}|[0-9a-zA-Z]{8})$");
    // 国航系编码规则：数字开头，数字和字母组成共8位长度
    private static final Pattern CA_PATTERN = Pattern.compile("^[0-9][A-Za-z0-9]{7}$");

    // 国航系全球客户：数字和字母组成的五位编码
    private static final Pattern CA_GLOBAL_PATTERN = Pattern.compile("^[A-Za-z0-9]{5}$");

    // 纯数字8位编码
    private static final Pattern DIGITAL_PATTERN = Pattern.compile("^[0-9]{8}$");

    // 虚拟手机号 170、171、162、165、167 开头
    private static final Pattern VIRTUAL_MOBILE = Pattern.compile("^(170|171|162|165|167)[0-9]+");

    // 受理地代码表
    private static final Map<String, String> ACCEPTANCE_CODE_MAP = new HashMap<>();

    static {
        ACCEPTANCE_CODE_MAP.put("11", "北京");
        ACCEPTANCE_CODE_MAP.put("12", "天津");
        ACCEPTANCE_CODE_MAP.put("13", "河北");
        ACCEPTANCE_CODE_MAP.put("14", "山西");
        ACCEPTANCE_CODE_MAP.put("15", "内蒙古");
        ACCEPTANCE_CODE_MAP.put("21", "辽宁");
        ACCEPTANCE_CODE_MAP.put("22", "吉林");
        ACCEPTANCE_CODE_MAP.put("23", "黑龙江");
        ACCEPTANCE_CODE_MAP.put("31", "上海");
        ACCEPTANCE_CODE_MAP.put("32", "江苏");
        ACCEPTANCE_CODE_MAP.put("33", "浙江");
        ACCEPTANCE_CODE_MAP.put("34", "安徽");
        ACCEPTANCE_CODE_MAP.put("35", "福建");
        ACCEPTANCE_CODE_MAP.put("36", "江西");
        ACCEPTANCE_CODE_MAP.put("37", "山东");
        ACCEPTANCE_CODE_MAP.put("41", "河南");
        ACCEPTANCE_CODE_MAP.put("42", "湖北");
        ACCEPTANCE_CODE_MAP.put("43", "湖南");
        ACCEPTANCE_CODE_MAP.put("44", "广东");
        ACCEPTANCE_CODE_MAP.put("45", "广西");
        ACCEPTANCE_CODE_MAP.put("46", "海南");
        ACCEPTANCE_CODE_MAP.put("50", "重庆");
        ACCEPTANCE_CODE_MAP.put("51", "四川");
        ACCEPTANCE_CODE_MAP.put("52", "贵州");
        ACCEPTANCE_CODE_MAP.put("53", "云南");
        ACCEPTANCE_CODE_MAP.put("54", "西藏");
        ACCEPTANCE_CODE_MAP.put("61", "陕西");
        ACCEPTANCE_CODE_MAP.put("62", "甘肃");
        ACCEPTANCE_CODE_MAP.put("63", "青海");
        ACCEPTANCE_CODE_MAP.put("64", "宁夏");
        ACCEPTANCE_CODE_MAP.put("65", "新疆");
        ACCEPTANCE_CODE_MAP.put("71", "台湾");
        ACCEPTANCE_CODE_MAP.put("81", "香港");
        ACCEPTANCE_CODE_MAP.put("82", "澳门");
    }

    // 校验码权重因子
    private static final int[] WEIGHT_FACTORS = {7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2};

    // 校验码对应表
    private static final char[] CHECK_CODE_TABLE = {'1', '0', 'X', '9', '8', '7', '6', '5', '4', '3', '2'};

    /**
     * 计算提前购票天数（只支持 "yyyy-MM-dd" 格式）
     * 如果预订日期晚于起飞日期，返回负数
     * 如果输入为空或格式错误，返回默认值
     *
     * @param bookDateStr   预订日期字符串（"yyyy-MM-dd"）
     * @param departDateStr 起飞日期字符串（"yyyy-MM-dd"）
     * @param defaultVal    格式错误或为空时返回的默认值
     * @return 提前购票天数（整数，向上取整）
     */
    public static int calculateAdvanceDays(String bookDateStr, String departDateStr, int defaultVal) {
        if (bookDateStr == null || departDateStr == null || bookDateStr.trim().isEmpty() || departDateStr.trim().isEmpty()) {
            return defaultVal;
        }

        try {
            LocalDate bookDate = LocalDate.parse(bookDateStr.trim());
            LocalDate departDate = LocalDate.parse(departDateStr.trim());

            return (int) ChronoUnit.DAYS.between(bookDate, departDate);
        } catch (DateTimeParseException e) {
            return defaultVal;
        }
    }

    /**
     * 获取证件类型
     *
     * @param type
     * @return
     */
    public static String getDocumentType(String type) {
        if (StringUtil.isEmpty(type)) {
            return "";
        }
        String doctype = "";
        switch (type) {
            case "NI":
                doctype = "ID";
                break;
            case "PSPT":
                doctype = "PP";
                break;
            default:
                doctype = "OT";
                break;
        }
        return doctype;
    }

    /**
     * 根据身份证号或出生日期计算年龄
     *
     * @param idCard      身份证号码（18位）
     * @param dateOfBirth 出生日期字符串（格式：yyyy-MM-dd）
     * @return 年龄，如果无法解析则返回 null
     */
    public static Integer getAgeByIdCard(String idCard, String dateOfBirth) {
        LocalDate birthDate = null;

        if (idCard != null && idCard.length() == 18) {
            try {
                String birthStr = idCard.substring(6, 14); // 提取出生日期字符串
                int year = Integer.parseInt(birthStr.substring(0, 4));
                int month = Integer.parseInt(birthStr.substring(4, 6));
                int day = Integer.parseInt(birthStr.substring(6, 8));
                birthDate = LocalDate.of(year, month, day);
            } catch (Exception e) {
                logger.error("身份证号码解析失败: {}", idCard, e);
            }
        } else if (dateOfBirth != null && !dateOfBirth.trim().isEmpty()) {
            try {
                birthDate = LocalDate.parse(dateOfBirth.trim());
            } catch (DateTimeParseException e) {
                logger.error("出生日期格式错误: {}", dateOfBirth, e);
            }
        }

        if (birthDate == null) {
            return null;
        }

        LocalDate now = LocalDate.now();
        return Period.between(birthDate, now).getYears();
    }


    /**
     * 获取停留天数
     *
     * @param segment
     * @param a
     * @param segmentList
     * @return
     */
    public static Integer getStopoverDay(Node segment, int a, NodeList segmentList) {
        // 当前段的航班日期
        String currentDepartureDate = XpathUtils.getString(segment, "Departure/@Date");

        // 获取下一段的航班日期（如果存在）
        String nextDepartureDate = null;
        if (a + 1 < segmentList.getLength()) {
            Node nextSegment = segmentList.item(a + 1);
            nextDepartureDate = XpathUtils.getString(nextSegment, "Departure/@Date");
        }
        Integer stopoverDay = null;
        if (segmentList.getLength() > 1) {
            if (currentDepartureDate != null && nextDepartureDate != null) {
                stopoverDay = TransUtils.calculateAdvanceDays(currentDepartureDate, nextDepartureDate, -1);
                if (stopoverDay < 0) {
                    stopoverDay = null; // 如果后一段早于当前段，视为无效
                }
            }
        }
        return stopoverDay;
    }

    /**
     * 获取季节标签
     *
     * @param date
     * @return
     */
    public static String getSeasonTag(String date) {
        String seasonTag = "";
        if (date != null && !date.isEmpty()) {
            try {
                String[] dateParts = date.split("-");
                if (dateParts.length >= 3) {
                    int month = Integer.parseInt(dateParts[1]);

                    if (month >= 3 && month <= 5) {
                        seasonTag = "春季";
                    } else if (month >= 6 && month <= 8) {
                        seasonTag = "夏季";
                    } else if (month >= 9 && month <= 11) {
                        seasonTag = "秋季";
                    } else {
                        seasonTag = "冬季";
                    }
                }
            } catch (Exception e) {
                logger.warn("解析起飞日期失败: {}", date, e);
            }
        }
        return seasonTag;
    }

    /**
     * 转换常客等级
     */
    public static String convertFfLevel(String isFfpJk) {
        if (isFfpJk == null) return null;
        switch (isFfpJk) {
            case "V":
                return "白金";
            case "G":
                return "金卡";
            case "S":
                return "银卡";
            case "C":
                return "普卡";
            case "B":
                return "黑卡"; // 需要确认业务含义
            default:
                return isFfpJk;
        }
    }

    /**
     * TODO 标准化转换
     * 转换舱位等级
     */
    public static String convertCabinClass(String sellClass) {
        // 更严格的空值和空字符串检查
        if (sellClass == null || sellClass.isEmpty()) {
            return null;
        }

        try {
            char firstChar = sellClass.charAt(0);
            switch (firstChar) {
                case 'F':
                    return "头等舱";
                case 'C':
                    return "公务舱";
                case 'J':
                    return "豪华公务舱";
                case 'Y':
                    return "经济舱";
                default:
                    return "其他";
            }
        } catch (StringIndexOutOfBoundsException e) {
            // 处理空字符串的情况
            logger.warn("舱位代码为空字符串: {}", sellClass);
            System.out.println("舱位代码为空字符串: " + sellClass);
            return null;
        }
    }


    /**
     * 根据日期和舱位代码返回舱等
     *
     * @param dateStr   日期字符串，格式为yyyy-MM-dd
     * @param sellClass 舱位代码（单个字符）
     * @return 舱等（公务/高经/经济）
     */
    public static String getCabinClass(String dateStr, String sellClass) {
        try {
            if (sellClass == null || sellClass.isEmpty()) {
                return null;
            }

            // 解析日期
            LocalDate date = LocalDate.parse(dateStr);
            LocalDate cutoffDate = LocalDate.parse("2023-10-29");

            // 取第一个字符作为舱位代码（假设sellClass可能是多字符字符串，如"YBM"）
            char cabinCode = sellClass.toUpperCase().charAt(0);

            // 根据日期范围判断舱等
            if (date.isBefore(cutoffDate)) {
                // 2022.1.1-2023.10.28期间的规则
                if ("CDPI".indexOf(cabinCode) != -1) {
                    return "BC";
                } else if ("WR".indexOf(cabinCode) != -1) {
                    return "PE";
                } else if ("YBHLQGVUZMKTSJEAOXN".indexOf(cabinCode) != -1) {
                    return "EC";
                }
            } else {
                // 2023.10.29及以后的规则
                if ("JCDRZI".indexOf(cabinCode) != -1) {
                    return "BC";
                } else if ("GE".indexOf(cabinCode) != -1) {
                    return "PE";
                } else if ("YBMUHQVWSLPNKTAOX".indexOf(cabinCode) != -1) {
                    return "EC";
                }
            }
        } catch (Exception e) {
            return null;
        }
        // 如果舱位代码不在任何规则中
        return null;

    }

    /**
     * 获取提前预订天数区间
     */
    public static String getAdvanceBookingRange(int days) {
        if (days <= 3) return "0-3天";
        if (days <= 7) return "4-7天";
        if (days <= 14) return "8-14天";
        if (days <= 30) return "15-30天";
        return "30天以上";
    }

    /**
     * 判断是否为大客户订单
     *
     * @param accountCode 大客户号
     * @return 是否符合大客户编码规则
     */
    public static boolean isKeyAccount(String accountCode) {
        if (accountCode == null || accountCode.trim().isEmpty()) {
            return false;
        }

        String code = accountCode.trim();

        // 检查是否符合任意一种规则
        return CA_PATTERN.matcher(code).matches() ||
                CA_GLOBAL_PATTERN.matcher(code).matches() ||
                DIGITAL_PATTERN.matcher(code).matches();
    }


    /**
     * 分解 VIP_TYPE_ID 并返回匹配的 typeIds
     *
     * @param vipTypeId 组合的 VIP 类型 ID（多个 2^n 之和）
     * @param typeIds   允许的 typeId 列表
     * @return 匹配的 typeId 列表（如果全部存在），否则返回空列表
     */
    public static List<Long> decomposeVipType(long vipTypeId, List<Long> typeIds) {
        // 1. 分解 vipTypeId 成 2 的 n 次方列表
        List<Long> decomposed = decomposeIntoPowersOfTwo(vipTypeId);

        // 2. 检查 decomposed 是否全部在 typeIds 中
        if (typeIds.containsAll(decomposed)) {
            return decomposed;
        } else {
            return new ArrayList<>(); // 或者抛出异常：throw new IllegalArgumentException("Invalid typeIds");
        }
    }

    /**
     * 将 long 值分解成 2 的 n 次方列表
     *
     * @param value 输入的值
     * @return 2 的 n 次方列表
     */
    private static List<Long> decomposeIntoPowersOfTwo(long value) {
        List<Long> result = new ArrayList<>();
        for (int bit = 0; bit < 64; bit++) {
            long mask = 1L << bit; // 计算 2^bit
            if ((value & mask) != 0) { // 检查该位是否为 1
                result.add(mask);
            }
        }
        return result;
    }
//    姓名：去除空格后保存，对于数据源中的姓名有小数点的，也需要入库。
//    统一英文名的姓和名的前后顺序。对于中文姓名对应的拼音姓名中的LV，
//    需统一转换成LYU。姓名需要去掉CHD\INF\MR\MS\GM\JC\VIP\VVIP\CIP等订座系统姓名后缀。
//    进行姓名的对比时，根据对比的数据源情况，有小数点则带着小数点比较，无小数点则去除小数点后比较。
    /**
     * 标准化姓名处理
     *
     * @param name 输入的姓名字符串
     * @return 标准化后的姓名数组，[0]为姓，[1]为名
     */
    /**
     * 规范化英文姓名，处理后缀、空格、姓名顺序及LV转换
     *
     * @param name 输入的英文姓名
     * @return 处理后的姓和名数组 [姓, 名]
     */
    public static String[] normalizeName(String name) {
        // 处理null或空字符串
        if (name == null || name.trim().isEmpty()) {
            return new String[]{null, null};
        }

        // 步骤1: 去除所有空格（保存时去除空格）
        String processedName = name.replaceAll("\\s+", "");

        // 步骤2: 定义需要去除的订座系统后缀（按长度倒序排列，避免部分匹配）
        List<String> suffixes = new ArrayList<>(Arrays.asList(
                "VVIP/", "CHD/", "INF/", "VIP/", "CIP/", "MR/", "MS/", "GM/", "JC/"
        ));

        // 循环去除所有可能的后缀（可能存在多个后缀）
        boolean hasSuffix;
        do {
            hasSuffix = false;
            for (String suffix : suffixes) {
                if (processedName.endsWith(suffix)) {
                    // 移除后缀
                    processedName = processedName.substring(0, processedName.length() - suffix.length());
                    hasSuffix = true;
                    break; // 移除后重新检查，避免连续后缀
                }
            }
        } while (hasSuffix);

        // 步骤3: 将拼音中的LV统一转换为LYU
        processedName = processedName.replace("LV", "LYU");

        // 步骤4: 根据/分割姓和名
        String[] parts = processedName.split("/");

        // 确保数组至少有两个元素，不足则用null补充
        parts = Arrays.copyOf(parts, 2);
        if (parts.length == 1) {
            parts[1] = null;
        }

        // 步骤5: 统一英文名的姓和名顺序（确保姓在前，名在后）
        // 处理分割后可能的空值情况
        String part1 = parts[0] != null && !parts[0].isEmpty() ? parts[0] : null;
        String part2 = parts[1] != null && !parts[1].isEmpty() ? parts[1] : null;

        // 英文名通常格式为"名/姓"，统一转换为"姓/名"
        String lastName = part1; // 姓
        String firstName = part2; // 名

        // 特殊情况处理：如果分割后只有一部分，默认作为姓
        if (lastName == null && firstName != null) {
            lastName = firstName;
            firstName = null;
        }

        return new String[]{lastName, firstName};
    }

    /**
     * 计算提前值机时间（整数小时）
     *
     * @param checkinTime 值机时间，格式为 "yyyy-MM-dd HH:mm:ss"（例如："2023-10-29 10:30:00"）
     * @param fltDate     航班日期，格式为 "yyyy-MM-dd"（例如："2023-10-29"）
     * @param dptm        起飞时间，格式为 "HH:mm:ss"（例如："14:30:00"）
     * @return 提前值机的整数小时（若值机时间晚于起飞时间，返回0）
     * @throws IllegalArgumentException 如果参数为null或格式错误
     */
    public static int calculateEarlyCheckinHours(String checkinTime, String fltDate, String dptm) {
        // 参数校验
        if (checkinTime == null || fltDate == null || dptm == null) {
            throw new IllegalArgumentException("参数不能为null" + checkinTime + "日期：" + fltDate + "起飞时间: " + dptm);
        }

        try {
            // 解析值机时间（精确到秒）
            LocalDateTime checkin = LocalDateTime.parse(
                    checkinTime,
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
            );

            // 组合航班日期和起飞时间（精确到秒）
            LocalDateTime departure = LocalDateTime.parse(
                    fltDate + " " + dptm,
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
            );

            // 计算时间差（秒→小时，取整）
            long secondsEarly = Duration.between(checkin, departure).getSeconds();
            int hoursEarly = (int) (secondsEarly / 3600); // 3600秒=1小时

            // 返回非负结果
            return Math.max(hoursEarly, 0);

        } catch (Exception e) {
            throw new IllegalArgumentException("时间格式错误，请确保输入格式为：\n" +
                    "- checkinTime: yyyy-MM-dd HH:mm:ss\n" +
                    "- fltDate: yyyy-MM-dd\n" +
                    "- dptm: HH:mm:ss");
        }
    }

    /**
     * 获取行李属性
     *
     * @param code 行李属性代码
     * @return 行李属性
     */
    public static String getBaggageAttr(String code) {
        if ("N".equals(code)) {
            code = "PC";
        } else if ("700".equals(code)) {
            code = "KG";
        }
        return code;
    }

    /**
     * 预付费行李额
     *
     * @param description 行李描述
     * @return 行李数量
     */
    public static Integer getQuantity(String description) {
        // 解析description字段获取数量
        int baggageQuantity = 0;
        if (description != null && !description.isEmpty()) {
            // 查找数字部分
            String[] parts = description.split(" ");
            if (parts.length > 0) {
                String quantityPart = parts[0]; // 获取第一个部分，如"1PC"
                // 提取数字
                String numericPart = quantityPart.replaceAll("[^0-9]", "");
                if (!numericPart.isEmpty()) {
                    baggageQuantity = Integer.parseInt(numericPart);
                }
            }
        }
        return baggageQuantity;
    }

    /**
     * 从文件读取上次的ID
     */
    public static long readLastId(String ID_FILE) {
        File file = new File(ID_FILE);
        if (!file.exists()) {
            return 0; // 文件不存在，从0开始
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line = reader.readLine();
            return line != null ? Long.parseLong(line.trim()) : 0;
        } catch (IOException | NumberFormatException e) {
            System.err.println("读取ID文件失败，将从0开始: " + e.getMessage());
            return 0;
        }
    }

    /**
     * 将最新ID写入文件
     */
    public static void writeLastId(long id, String ID_FILE) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(ID_FILE))) {
            writer.write(String.valueOf(id));
        } catch (IOException e) {
            System.err.println("写入ID文件失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 获取TID
     *
     * @param documentNumber 证件号码
     * @param documentType   证件类型
     * @return TID
     */
    public static String getTid(String documentNumber, String documentType, String dataType) {
        if(StringUtils.isNotBlank(documentNumber)){
            return SM4Utils.encrypt(documentNumber, Constants.SM4_KEY);
        }
        return "";

        /*Tid tid = new Tid();
        tid.setTid(SM4Utils.encrypt(documentNumber, Constants.SM4_KEY));
        String id_type = "";
        if ("TRP".equals(dataType)) {
            id_type = NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, documentType, DataSource.TRP);
        }
        if ("HSD".equals(dataType)) {
            id_type = NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, documentType, DataSource.HIGH_FREQUENCY_DATA);
        }
        String cert_no = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, SM4Utils.encrypt(documentNumber, Constants.SM4_KEY));
        HashMap<String, String> map = new HashMap<>();
        map.put(id_type, cert_no);
        tid.setCertification(map);
        return IdMapping.idMappingFunction(tid, dataType, false,false,false);*/
    }

    /**
     * TRP获取预订人TID
     *
     * @param customerId 直销CUSTOMER_ID
     * @return TID
     */
    public static String getBookUserTid(String customerId) {
        Tid tid = new Tid();
        String dataType = "TRP";
        tid.setTid(SM4Utils.encrypt(customerId, Constants.SM4_KEY));
        tid.setCrmCustomerId(SM4Utils.encrypt(customerId, Constants.SM4_KEY));
        return IdMapping.idMappingFunction(tid, dataType);
    }

    /**
     * 从DOCS类型的SpecialServiceRequest中提取证件号码
     * 举例：P/CN/EQ1158847/CN/25JUL05/F/26FEB26/QIN/MENGTINGMS/H
     *
     * @param segment    segment节点
     * @param textPrefix Text字段的前缀标识（如"P"表示护照，"F"表示外国人出入境证等）
     * @param position   证件号码在分割后的数组中的位置（从0开始）
     * @return 提取到的证件号码，未找到则返回null
     */
    public static String extractDocIdFromSSR(Node segment, String textPrefix, int position) {
        String text = XpathUtils.getString(segment,
                "SpecialServiceRequest[SSRCode='DOCS' and Text[starts-with(., '" + textPrefix + "')]]/Text");
        if (text != null && !text.isEmpty()) {
            String[] parts = text.split("/");
            if (parts.length > position) {
                return parts[position];
            }
        }
        return null;
    }

    /**
     * 获取大客户号
     *
     * @param segment 航段节点
     * @return String 大客户号
     */
    public static String getKeyAccount(Node segment) {
        // 大客户号
        String largeCustomerNumber = null;
        NodeList ckinSSRList = XpathUtils.getNodeList(segment, "SpecialServiceRequest[SSRCode='CKIN']");
        for (int i = 0; i < ckinSSRList.getLength(); i++) {
            Node ssrNode = ckinSSRList.item(i);
            String text = XpathUtils.getString(ssrNode, "Text");
            String actionCode = XpathUtils.getString(ssrNode, "ActionCode");

            if (StringUtils.isNotBlank(text)) {
                // 情况1: Text中包含"HK1"且格式为"VICOCKIN HK1 VICO99992001"或5位编码
                if (text.contains("HK1")) {
                    int vicoIndex = text.indexOf("VICO");
                    if (vicoIndex != -1 && vicoIndex + 4 < text.length()) {
                        String codePart = text.substring(vicoIndex + 4);
                        // 检查5位编码
                        if (codePart.length() >= 5) {
                            String potentialCode = codePart.substring(0, 5);
                            if (CA_GLOBAL_PATTERN.matcher(potentialCode).matches()) {
                                largeCustomerNumber = potentialCode;
                                break;
                            }
                        }
                        // 检查8位编码（必须数字开头）
                        if (codePart.length() >= 8) {
                            String potentialCode = codePart.substring(0, 8);
                            if (CA_PATTERN.matcher(potentialCode).matches()) {
                                largeCustomerNumber = potentialCode;
                                break;
                            }
                        }
                    }
                }
                // 情况2: Text包含"VICO"且ActionCode为"HK1"
                else if ("HK1".equals(actionCode)) {
                    int vicoIndex = text.indexOf("VICO");
                    if (vicoIndex != -1 && vicoIndex + 4 < text.length()) {
                        String codePart = text.substring(vicoIndex + 4);
                        // 检查5位编码
                        if (codePart.length() == 5) {
                            String potentialCode = codePart.substring(0, 5);
                            if (CA_GLOBAL_PATTERN.matcher(potentialCode).matches()) {
                                largeCustomerNumber = potentialCode;
                                break;
                            }
                        }
                        // 检查8位编码（必须数字开头）
                        if (codePart.length() == 8) {
                            String potentialCode = codePart.substring(0, 8);
                            if (CA_PATTERN.matcher(potentialCode).matches()) {
                                largeCustomerNumber = potentialCode;
                                break;
                            }
                        }
                    }
                }

            }
        }
        return largeCustomerNumber;
    }

    /**
     * 是否虚拟手机
     *
     * @param mobile 加密手机号
     * @return
     */
    public static boolean isVirtualMobile(String mobile) {
        // 解密
        String decryptStr = SM4Utils.decrypt(mobile, Constants.SM4_KEY);
        Matcher mobileMatcher = VIRTUAL_MOBILE.matcher(decryptStr);
        if (mobileMatcher.find()) {
            // 匹配到 true
            return true;
        } else {
            return false;
        }
    }

    /**
     * 直销用户 DataStream 预处理 获取tid
     *
     * @param rowStream
     * @return
     */
    public static DataStream<Row> zxyhPrepareHandle(DataStream<Row> rowStream) {
        // 标准化 IdMapping
        DataStream<Row> rowDataStream = rowStream.map(row -> {
            // 获取tid
            Tid tid = new Tid();
            tid.setTid(SM4Utils.encrypt(row.getField("CUSTOMER_ID").toString(), Constants.SM4_KEY));
            tid.setCrmCustomerId(SM4Utils.encrypt(row.getField("CUSTOMER_ID").toString(), Constants.SM4_KEY));
            String tidStr = IdMapping.idMappingFunction(tid, "ZXYH");
            Row rowModel = Row.copy(row);
            rowModel.setField("TID", tidStr);

            return rowModel;
        });
        return rowDataStream;
    }

    /**
     * clk DataStream 预处理 获取tid
     *
     * @param memberNo
     * @return
     */
    public static String getClkTid(String memberNo) {
        // 获取tid
        Tid tid = new Tid();
        tid.setTid(memberNo);
        tid.setFrequentTravelerCardno(memberNo);
        String tidStr = IdMapping.idMappingFunction(tid, "FFP");
        return tidStr;
    }

    /**
     * 用户维表证件号识别
     *
     * @param model      用户维表对象
     * @param certType   证件类型 标准化后
     * @param certNumber 证件号 标准化后
     */
    public static void userDimCertHandle(UserDimModel model, String certType, String certNumber) {
        if (null == certType || null == certNumber) {
            return;
        }

        switch (certType) {
            case "ID":
                // 身份证
                model.setIdCard(certNumber);
                break;
            case "PP":
                // 护照
                model.setPassport(certNumber);
                break;
            case "SC":
                // 海员证
                model.setSeamanId(certNumber);
                break;
            case "FR":
                // 外国人出入境证
                model.setAlienPermit(certNumber);
                break;
            case "CD":
                // 外交部签发的驻华外交人员证
                model.setDiplomaticStaffCertificate(certNumber);
                break;
            case "PR":
                // 外国人永久居留证
                model.setPermanentResidentId(certNumber);
                break;
            case "CS":
                // 文职人员证
                model.setCivilianStaffId(certNumber);
                break;
            case "SE":
                // 职工证
                model.setStaffId(certNumber);
                break;
            case "OF":
                // 军官证
                model.setOfficerIdCard(certNumber);
                break;
            case "WP":
                // 武警警官证
                model.setArmedPoliceOfficer(certNumber);
                break;
            case "WS":
                // 武警士兵证
                model.setArmedPoliceSoldier(certNumber);
                break;
            case "CW":
                // 文职干部证
                model.setCivilianOfficialId(certNumber);
                break;
            case "VC":
                // 义务兵证
                model.setConscriptSoldierId(certNumber);
                break;
            case "NC":
                // 士官证
                model.setNonCommissionedOfficerId(certNumber);
                break;
            case "RP":
                // 港澳居民来往内地通行证
                model.setHkMacaoResidentPermit(certNumber);
                break;
            case "TC":
                // 台湾居民来往大陆通行证
                model.setTaiwanResidentTravelPermit(certNumber);
                break;
            default:
                break;
        }

    }

    /**
     * 航段级解读卡券信息 TODO 逻辑处理
     */
    public static Map<String, Object> segCoupon(Node paymentDetails, String ojSuperPnrRph,
                                                String flightSegmentRefNumber) {
        Map<String, Object> map = new HashMap<>();
        //卡券信息
        //找到使用了卡券的payment节点集合
        NodeList paymentList = XpathUtils.getNodeList(paymentDetails, "ota:Payments/ota:Payment[@Historic='false' and @TransactionType='Debit' and ota:PaymentForm/ota:Other[@Type = '211']]");
        //是否使用优惠券 初始值为false
        String akCoupon = "N";
        //优惠券活动名称
        String akCouponCode = "";
        //优惠券类型
        String akCouponType = "";
        String couponNo = "";
        String akCouponName = "";
        //定义一个set放couponNo
        Set<String> set = new HashSet<>();
        //优惠金额
        double couponAmt = 0.0;
        for (int i = 0; i < paymentList.getLength(); i++) {
            Node payment = paymentList.item(i);
            NodeList paymentFormList = XpathUtils.getNodeList(payment, "ota:PaymentForm[ota:Other/@Type = '211']");
            for (int j = 0; j < paymentFormList.getLength(); j++) {
                Node paymentForm = paymentFormList.item(j);
                if (ojSuperPnrRph.equals(XpathUtils.getString(paymentForm, "ota:Other/ota:Ref[@Code = 'OJ_SuperPNR_RPH']")) &&
                        flightSegmentRefNumber.equals(XpathUtils.getString(paymentForm, "ota:Other/ota:Ref[@Code = 'FlightSegmentRefNumber']"))) {
                    couponAmt = couponAmt + XpathUtils.getNumber(paymentForm, "ota:Other/ota:Ref[@Code='PassengerAmount']");
                    Node other = XpathUtils.getNode(paymentForm, "ota:Other");
                    //获取航段
                    akCoupon = "Y";
                    couponNo = XpathUtils.getString(other, "ota:Ref[@Code = 'couponNo']");
                    set.add(couponNo);
                    akCouponCode = XpathUtils.getString(other, "ota:Ref[@Code = 'couponCode']");
                    akCouponType = XpathUtils.getString(other, "ota:Ref[@Code = 'type']");
                    akCouponName = XpathUtils.getString(other, "ota:Ref[@Code = 'couponName']");
                }
            }
        }
        map.put("akCoupon", akCoupon);
        map.put("akCouponCode", akCouponCode);
        map.put("akCouponType", akCouponType);
        map.put("couponCount", set.size());
        map.put("couponAmt", couponAmt);
        map.put("couponNo", couponNo);
        map.put("couponName", akCouponName);
        return map;
    }

    /**
     * 将 UTC 时间字符串（格式：yyyy-MM-dd'T'HH:mm:ss）转换为北京时间（UTC+8）
     *
     * @param utcTimeStr UTC 时间字符串，如 "2025-08-12T12:23:45"
     * @return 北京时间字符串，格式为 "yyyy-MM-dd HH:mm:ss"
     */
    public static String convertUtcToBeijingTime(String utcTimeStr) {
        if (utcTimeStr == null || utcTimeStr.isEmpty()) {
            return "";
        }
        try {
            // 定义输入格式
            DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
            // 解析为 LocalDateTime（按 UTC 理解）
            LocalDateTime utcTime = LocalDateTime.parse(utcTimeStr, inputFormatter);
            // 加上 8 小时得到北京时间
            LocalDateTime beijingTime = utcTime.plusHours(8);
            // 定义输出格式
            DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
            // 返回格式化后的字符串
            return beijingTime.format(outputFormatter);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid date format: " + utcTimeStr, e);
        }
    }

    /**
     * 应退金额 ,退票手续费 退票航段级事实表使用
     *
     * @param list
     * @return
     */
    public static Double getAmount(NodeList list) {
        Double amount = 0.0;
        for (int a = 0; a < list.getLength(); a++) {
            Node refundAmountNode = list.item(a);
            Double number = XpathUtils.getNumber(refundAmountNode, "@QualifierValue");
            if (number == null) {
                number = 0.0;
            }
            amount = amount + number;
        }
        return amount;

    }

    /**
     * * 根据值机Agent 转换为 值机渠道
     *
     * @param agent
     * @return
     */
    public static String convertAgentToCode(String agent) {
        if (agent == null) {
            return null;
        }

        switch (agent.trim()) {
            case "70810":
                return "SCIBE";    // 官网
            case "70812":
                return "SCXCX";   // 小程序（支付宝/微信）
            case "70807":
                return "SCZSF";    // 掌尚飞APP
            case "70811":
                return "SCWEX";    // 微信公众号
            default:
                return agent;      // 未知类型返回原值，或者可以抛异常/返回null
        }
    }

    /**
     * 判断时间是否与当前时间相差超过1小时-TRP（精确到秒）
     *
     * @param sourceLastUpdateTime 时间字符串，格式为: 2022-04-03T08:15:27
     * @return true-超过1小时，false-未超过1小时
     */
    public static boolean isExceedingOneHour(String sourceLastUpdateTime) {
        if (sourceLastUpdateTime == null || sourceLastUpdateTime.isEmpty()) {
            return true;
        }

        try {
            // 解析时间字符串
            DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
            java.time.LocalDateTime sourceTime = java.time.LocalDateTime.parse(sourceLastUpdateTime, formatter);
            java.time.LocalDateTime currentTime = java.time.LocalDateTime.now();

            // 计算时间差（秒数）
            long secondsDifference = Math.abs(java.time.Duration.between(sourceTime, currentTime).getSeconds());

            // 判断是否超过3600秒（1小时）
            boolean isExceeding = secondsDifference > 3600;

            //logger.info("时间比较结果: 源时间={}, 当前时间={}, 相差秒数={}, 是否超过一小时={}",sourceTime, currentTime, secondsDifference, isExceeding);

            return isExceeding;
        } catch (Exception e) {
            logger.warn("时间解析失败: sourceLastUpdateTime={}, 错误信息={}", sourceLastUpdateTime, e.getMessage());
            return true;
        }
    }

    /**
     * 统一校验方法
     *
     * @param idNumber 证件号码
     * @return 校验结果 true-有效, false-无效
     */
    public static boolean isCodeRuleCompliant(String idNumber) {
        if (idNumber == null || idNumber.trim().isEmpty()) {
            return false;
        }
        try {
            // 解密
            idNumber = SM4Utils.decrypt(idNumber, Constants.SM4_KEY);
        } catch (Exception e) {
            logger.error("证件号码解密失败: idNumber={}, 错误信息={}", idNumber, e.getMessage());
            return false;
        }
        // 移除可能存在的空格
        idNumber = idNumber.trim();

        // 判断证件类型并校验
        if (idNumber.length() == 18) {
            char firstChar = idNumber.charAt(0);
            if (firstChar == '9') {
                return validateNewPermanentResidentCard(idNumber);
            } else if (firstChar >= '1' && firstChar <= '8') {
                return validateIdCard(idNumber);
            } else {
                return false;
            }
        } else if (idNumber.length() == 15) {
            return validateOldIdCard(idNumber);
        } else {
            return false;
        }
    }

    /**
     * 校验新版永居证
     */
    private static boolean validateNewPermanentResidentCard(String cardNumber) {
        // 1. 检查首位是否为9
        if (cardNumber.charAt(0) != '9') {
            return false;
        }

        // 2. 检查受理地代码
        String acceptanceCode = cardNumber.substring(1, 3);
        if (!ACCEPTANCE_CODE_MAP.containsKey(acceptanceCode)) {
            return false;
        }

        // 3. 检查国家/地区代码
        String countryCode = cardNumber.substring(3, 6);
        if (!Pattern.matches("\\d{3}", countryCode)) {
            return false;
        }

        // 4. 检查出生日期
        String birthDate = cardNumber.substring(6, 14);
        if (!isValidDate(birthDate)) {
            return false;
        }

        // 5. 检查顺序码
        String sequenceCode = cardNumber.substring(14, 17);
        if (!Pattern.matches("\\d{3}", sequenceCode)) {
            return false;
        }

        // 6. 检查校验码
        return validateCheckCode(cardNumber);
    }

    /**
     * 校验18位身份证
     */
    private static boolean validateIdCard(String idNumber) {
        // 1. 检查地址码
        String addressCode = idNumber.substring(0, 6);
        if (!Pattern.matches("\\d{6}", addressCode)) {
            return false;
        }

        // 2. 检查出生日期
        String birthDate = idNumber.substring(6, 14);
        if (!isValidDate(birthDate)) {
            return false;
        }

        // 3. 检查顺序码
        String sequenceCode = idNumber.substring(14, 17);
        if (!Pattern.matches("\\d{3}", sequenceCode)) {
            return false;
        }

        // 4. 检查校验码
        return validateCheckCode(idNumber);
    }

    /**
     * 校验15位旧身份证
     */
    private static boolean validateOldIdCard(String idNumber) {
        // 1. 检查地址码
        String addressCode = idNumber.substring(0, 6);
        if (!Pattern.matches("\\d{6}", addressCode)) {
            return false;
        }

        // 2. 检查出生日期
        String birthDate = "19" + idNumber.substring(6, 12);
        if (!isValidDate(birthDate)) {
            return false;
        }

        // 3. 检查顺序码
        String sequenceCode = idNumber.substring(12, 15);
        return Pattern.matches("\\d{3}", sequenceCode);
    }

    /**
     * 校验日期格式
     */
    private static boolean isValidDate(String dateStr) {
        if (dateStr == null || dateStr.length() != 8) {
            return false;
        }

        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            sdf.setLenient(false); // 严格模式
            sdf.parse(dateStr);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    /**
     * 校验码验证
     */
    private static boolean validateCheckCode(String idNumber) {
        if (idNumber.length() != 18) {
            return false;
        }

        int sum = 0;
        for (int i = 0; i < 17; i++) {
            sum += (idNumber.charAt(i) - '0') * WEIGHT_FACTORS[i];
        }

        int remainder = sum % 11;
        char checkCode = CHECK_CODE_TABLE[remainder];

        return checkCode == idNumber.charAt(17);
    }

    /**
     * 获取附加服务详情中原始舱位
     */
    public static String getOriginalCabin(String description) {
        if (description == null || description.isEmpty()) {
            return null;
        }
        String[] parts = description.split(" ");
        return parts.length > 2 ? parts[2] : null;
    }

    /**
     * 解析选座详情字段，提取座位区域代码和座位号
     * 示例："G 07A CNY0 A" -> 区域代码="G", 座位号="07A"
     * 规则：首字母为区域代码，首字母空格后的内容（两空格之间）为座位号
     *
     * @param description 选座详情字段
     * @return String数组，[0]为区域代码，[1]为座位号，如果解析失败返回null
     */
    public static String[] parseSeatInfo(String description) {
        if (description == null || description.trim().isEmpty()) {
            return null;
        }
        String trimmed = description.trim();
        if (trimmed.length() == 0) {
            return null;
        }
        String seatZone = String.valueOf(trimmed.charAt(0));
        String[] parts = trimmed.split("\\s+");
        String seatNumber = null;
        if (parts.length > 1) {
            seatNumber = parts[1];
        }
        return new String[]{seatZone, seatNumber};
    }

    /**
     * 空值安全转换为字符串
     */
    public static String toStringSafe(Object obj) {
        return obj != null ? obj.toString() : null;
    }

    /**
     * 将格式为 YYYYMMDDHHMMSSsss 的时间字符串转换为 Timestamp 格式
     * @param uptm 时间字符串，格式如 "20251009050538383"
     * @return 转换后的时间字符串，格式为 "yyyy-MM-dd HH:mm:ss.SSS"，如果转换失败则返回原字符串
     */
    public static String parseUptmToTimestamp(String uptm) {
        if (uptm == null || uptm.length() != 17) { // 验证格式：YYYYMMDDHHMMSSsss
            logger.warn("uptm 格式不正确: {}", uptm);
            return uptm;
        }

        try {
            // 将格式从 YYYYMMDDHHMMSSsss 转换为 YYYY-MM-DD HH:MM:SS.sss
            String formattedStr = String.format("%s-%s-%s %s:%s:%s.%s",
                    uptm.substring(0, 4),   // 年
                    uptm.substring(4, 6),   // 月
                    uptm.substring(6, 8),   // 日
                    uptm.substring(8, 10),  // 时
                    uptm.substring(10, 12), // 分
                    uptm.substring(12, 14), // 秒
                    uptm.substring(14, 17)  // 毫秒
            );

            java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(formattedStr);
            return String.valueOf(timestamp);
        } catch (Exception e) {
            logger.warn("uptm 格式错误: {}", uptm);
            return uptm;
        }
    }

}
