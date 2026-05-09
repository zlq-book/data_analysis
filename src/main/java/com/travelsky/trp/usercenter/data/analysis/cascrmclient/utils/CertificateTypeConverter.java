package com.travelsky.trp.usercenter.data.analysis.cascrmclient.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 证件类型转换工具类
 */
public class CertificateTypeConverter {

    // 证件类型映射关系
    private static final Map<String, String> CERT_TYPE_MAP = new HashMap<>();
    private static final Map<String, String> CERT_TYPE_MAP_2 = new HashMap<>();
    // 公务护照前缀正则表达式
    private static final Pattern OFFICIAL_PASSPORT_PATTERN = Pattern.compile("^(SE|DE|PE)\\d{7}$");

    static {
        // 初始化映射关系
        // Others 类型
        CERT_TYPE_MAP.put("HR", "Others");
        CERT_TYPE_MAP.put("BC", "Others");
        CERT_TYPE_MAP.put("SI", "Others");
        CERT_TYPE_MAP.put("PI", "Others");
        CERT_TYPE_MAP.put("CT", "Others");
        CERT_TYPE_MAP.put("RT", "Others");
        CERT_TYPE_MAP.put("RR", "Others");
        CERT_TYPE_MAP.put("FR", "Others");
        CERT_TYPE_MAP.put("CD", "Others");
        CERT_TYPE_MAP.put("SC", "Others");
        CERT_TYPE_MAP.put("IS", "Others");
        CERT_TYPE_MAP.put("SE", "Others");
        CERT_TYPE_MAP.put("ST", "Others");
        CERT_TYPE_MAP.put("OT", "Others");
        CERT_TYPE_MAP.put("UU", "Others");
        CERT_TYPE_MAP.put("ID", "Others");

        // Reentry_Permit 类型
        CERT_TYPE_MAP.put("RP", "Reentry_Permit");

        // HK_Macao_Permit 类型
        CERT_TYPE_MAP.put("HP", "HK_Macao_Permit");

        // Taiwan_ID 类型
        CERT_TYPE_MAP.put("TC", "Taiwan_ID");

        // Officer_Certification 类型
        CERT_TYPE_MAP.put("OF", "Officer_Certification");
        CERT_TYPE_MAP.put("SM", "Officer_Certification");
        CERT_TYPE_MAP.put("PO", "Officer_Certification");
        CERT_TYPE_MAP.put("WP", "Officer_Certification");
        CERT_TYPE_MAP.put("WS", "Officer_Certification");
        CERT_TYPE_MAP.put("MP", "Officer_Certification");
        CERT_TYPE_MAP.put("CW", "Officer_Certification");
        CERT_TYPE_MAP.put("CS", "Officer_Certification");
        CERT_TYPE_MAP.put("VC", "Officer_Certification");
        CERT_TYPE_MAP.put("NC", "Officer_Certification");
        CERT_TYPE_MAP.put("PE", "Officer_Certification");
        CERT_TYPE_MAP.put("DP", "Officer_Certification");

        // PR Card 类型
        CERT_TYPE_MAP.put("PR", "PR Card");

        // ID Card 类型
        CERT_TYPE_MAP.put("NI", "ID Card");

        // 护照类型特殊处理
        CERT_TYPE_MAP.put("PSPT", "PASSPORT");
        CERT_TYPE_MAP.put("PP", "PASSPORT");
    }

    static {
        // 初始化映射关系
        // Others 类型
        CERT_TYPE_MAP_2.put("ID Card", "NI");
        CERT_TYPE_MAP_2.put("Taiwan_Permit", "OT");
        CERT_TYPE_MAP_2.put("U.N._Passport", "PP");
        CERT_TYPE_MAP_2.put("Official_Passport", "PP");
        CERT_TYPE_MAP_2.put("HK_Macao_Permit", "HP");
        CERT_TYPE_MAP_2.put("Offical_HK_Macao", "HP");
        CERT_TYPE_MAP_2.put("Officer_Certification", "OF");
        CERT_TYPE_MAP_2.put("Taiwan_ID", "TC");
        CERT_TYPE_MAP_2.put("Reentry_Permit", "RP");
        CERT_TYPE_MAP_2.put("Ordinary_Passport", "PP");
        CERT_TYPE_MAP_2.put("Others", "OT");
        CERT_TYPE_MAP_2.put("TW_ID_Card", "RT");
        CERT_TYPE_MAP_2.put("HK_ID_Card", "RT");
        CERT_TYPE_MAP_2.put("MACAO_ID_Card", "RT");
        CERT_TYPE_MAP_2.put("PR Card", "PR");
    }

    public static String convertCertificateType2(String certTypeCode) {
        return CERT_TYPE_MAP_2.getOrDefault(certTypeCode, "OT");
    }



    /**
     * 证件类型转换方法
     * @param certNo 证件号码
     * @param certTypeCode 证件类型代码
     * @return 转换后的证件类型
     */
    public static String convertCertificateType(String certNo, String certTypeCode) {
        if (certTypeCode == null || certTypeCode.trim().isEmpty()) {
            return "Others";
        }

        // 统一转换为大写，去除空格
        String code = certTypeCode.trim().toUpperCase();

        // 特殊处理：ID 类型（需根据证件号判断是否为身份证）
        if ("ID".equals(code)) {
            return isChineseIDCard(certNo) ? "ID Card" : "Others";
        }

        // 获取基础映射
        String result = CERT_TYPE_MAP.getOrDefault(code, "Others");

        // 特殊处理护照类型
        if ("PASSPORT".equals(result)) {
            return classifyPassport(certNo);
        }

        return result;
    }

    /**
     * 判断是否为身份证号码
     */
    private static boolean isChineseIDCard(String certNo) {
        if (certNo == null || certNo.trim().isEmpty()) {
            return false;
        }

        String idCard = certNo.trim();

        // 基本长度检查：15位或18位
        if (idCard.length() != 15 && idCard.length() != 18) {
            return false;
        }

        // 正则表达式检查
        String pattern15 = "^[1-9]\\d{7}((0\\d)|(1[0-2]))(([0|1|2]\\d)|3[0-1])\\d{3}$";
        String pattern18 = "^[1-9]\\d{5}(18|19|20)\\d{2}((0[1-9])|(1[0-2]))(([0-2][1-9])|10|20|30|31)\\d{3}[0-9Xx]$";

        return idCard.matches(pattern15) || idCard.matches(pattern18);
    }

    /**
     * 护照类型细分：区分普通护照和公务护照
     */
    private static String classifyPassport(String certNo) {
        if (certNo == null || certNo.trim().isEmpty()) {
            return "Ordinary_Passport";
        }

        String passportNo = certNo.trim().toUpperCase();

        // 检查是否为公务护照（SE/DE/PE + 7位数字）
        if (OFFICIAL_PASSPORT_PATTERN.matcher(passportNo).matches()) {
            return "Official_Passport";
        }

        return "Ordinary_Passport";
    }

    /**
     * 批量转换方法（可选）
     */
    public static Map<String, String> batchConvert(Map<String, String> certData) {
        Map<String, String> result = new HashMap<>();

        for (Map.Entry<String, String> entry : certData.entrySet()) {
            String certNo = entry.getKey();
            String certType = convertCertificateType(certNo, entry.getValue());
            result.put(certNo, certType);
        }

        return result;
    }
    // 使用静态 Map
    private static final Map<String, String> CARD_MAP = Stream.of(new String[][] {
            {"Gold", "G"},
            {"Junior", "J"},
            {"Lifetime Platinum", "L"},
            {"Premium Lifetime Platinum", "T"},
            {"Normal", "B"},
            {"Platinum", "V"},
            {"Silver", "S"}
    }).collect(Collectors.toMap(data -> data[0], data -> data[1]));

    public static String getCardCode(String englishName) {
        return CARD_MAP.get(englishName);
    }

}
