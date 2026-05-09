package com.travelsky.dataplatform.utils;

import com.travelsky.dataplatform.constans.Constants;
import org.apache.flink.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NormalizationUtils {

    private static final Map<String, Map<String, String>> MAPPINGS = new HashMap<>();
    private static final String CONFIG_FILE = "fieldmappings.properties";

    static {
        loadMappingsFromProperties();
    }

    private static void loadMappingsFromProperties() {
        Properties properties = new Properties();
        try (InputStream input = NormalizationUtils.class.getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            InputStreamReader reader = new InputStreamReader(input, StandardCharsets.UTF_8);
            if (input == null) {
                System.out.println("无法找到配置文件: " + CONFIG_FILE);
                return;
            }
            properties.load(reader);

//            for (FieldType fieldType : FieldType.values()) {
//                MAPPINGS.put(fieldType, new HashMap<>());
//            }

            for (String key : properties.stringPropertyNames()) {
                String[] parts = key.split("\\.", 2); // 分割FieldType和键值
                if (parts.length == 2) {
                    String fieldTypeName = parts[0];
                    String originalValue = parts[1];
                    String standardizedValue = properties.getProperty(key);

                    try {
//                        FieldType fieldType = FieldType.valueOf(fieldTypeName);
                        if (MAPPINGS.get(fieldTypeName) == null) {
                            MAPPINGS.put(fieldTypeName, new HashMap<>());
                        }
                        MAPPINGS.get(fieldTypeName).put(originalValue.trim().replaceAll("!", " ").toUpperCase(), standardizedValue.trim());
                    } catch (IllegalArgumentException e) {
                        System.err.println("忽略无效的字段类型: " + fieldTypeName);
                    }
                } else {
                    System.err.println("忽略无效的配置项: " + key);
                }
            }

        } catch (IOException e) {
            System.err.println("加载配置文件出错: " + e.getMessage());
        }
    }

    /**
     * 标准化字段类型
     *
     * @param fieldType     字段类型枚举
     * @param originalValue 原始值
     * @return 标准化后的值，如果找不到则返回原始值
     */
    public static String standardize(FieldType fieldType, String originalValue, DataSource... dataSource) {
        if (fieldType == null || originalValue == null || "".equals(originalValue) || "null".equals(originalValue)) {
            return null;
        }
        if(fieldType.equals(FieldType.DOCUMENT_TYPE) ||
                fieldType.equals(FieldType.CHECK_IN_METHOD)||
                fieldType.equals(FieldType.CASH_PAYMENT_PLATFORM)||
                fieldType.equals(FieldType.ORDER_PAYMENT_METHOD)||
                fieldType.equals(FieldType.INSURANCE_ORDER_CHANNEL)||
                fieldType.equals(FieldType.INSURANCE_STATUS)) {
            if (dataSource == null || dataSource.length < 1) {
                throw new IllegalArgumentException(fieldType.name()+" 需要指定数据源，调用方式: NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, documentType, DataSource.ZHANGSHANGFEI)");
            }
        }

        String newValue = originalValue;
        // 证件号码，手机号码，常客卡号需要先解密处理
        if(fieldType.equals(FieldType.DOCUMENT_NUM) ||
                fieldType.equals(FieldType.MOBILE_NO) ||
                fieldType.equals(FieldType.FREQUENT_FLYER_NUM)) {
            newValue = SM4Utils.decrypt(newValue, Constants.SM4_KEY);
        }
        if (newValue == null || "".equals(newValue) || "null".equals(newValue)) {
            return null;
        }

        // 证件号，手机号，常旅客卡号，姓名，国籍，省份，城市，民族，机场，票号，国际国内标识处理
        if(fieldType.equals(FieldType.DOCUMENT_NUM)) {
            newValue = newValue.trim().toUpperCase();
        } else if(fieldType.equals(FieldType.MOBILE_NO)) {
            newValue = newValue.replaceAll("\\s+", "");
        } else if(fieldType.equals(FieldType.FREQUENT_FLYER_NUM)) {
            newValue = newValue.trim().replaceAll("\\D", "");;
        } else if(fieldType.equals(FieldType.EN_NAME) || fieldType.equals(FieldType.CN_NAME)) {
            newValue = removeNameSuffix(newValue);
            newValue = removeInfantSuffix(newValue);
            newValue = newValue.toUpperCase();
        } else if(fieldType.equals(FieldType.NAME_PINYIN)) {
            newValue = newValue.replaceAll("LV", "LYU");
            newValue = newValue.replaceAll("LU:", "LYU");
        } else if(fieldType.equals(FieldType.TICKET_NO)) {
            newValue = newValue.replaceAll("-", "");
        } else {
            // 获取KEY
            String mapkey = fieldType.name();
            if(fieldType.equals(FieldType.DOCUMENT_TYPE) ||
                    fieldType.equals(FieldType.CHECK_IN_METHOD)||
                    fieldType.equals(FieldType.CASH_PAYMENT_PLATFORM)||
                    fieldType.equals(FieldType.ORDER_PAYMENT_METHOD)||
                    fieldType.equals(FieldType.INSURANCE_ORDER_CHANNEL)||
                    fieldType.equals(FieldType.INSURANCE_STATUS)) {
                if (dataSource != null && dataSource.length > 0) {
                    mapkey = fieldType.name() + "-" + dataSource[0].name();
                }
                if (dataSource.length > 1) {
                    mapkey = mapkey + "-" + dataSource[1].name();
                }
            }
            // 处理字段映射
            Map<String, String> mapping = MAPPINGS.get(mapkey);
            if (mapping != null) {
                // 默认值
                String defaultValue = mapping.getOrDefault("default", newValue);
                newValue = mapping.getOrDefault(newValue.trim().toUpperCase(), defaultValue);
            }
        }

        // 证件号码，手机号码，常客卡号需要加密处理
        if(fieldType.equals(FieldType.DOCUMENT_NUM) ||
                fieldType.equals(FieldType.MOBILE_NO) ||
                fieldType.equals(FieldType.FREQUENT_FLYER_NUM)) {
            newValue = SM4Utils.encrypt(newValue, Constants.SM4_KEY);
        }

        return newValue;
    }

    public static void main(String[] args) {
        // 示例用法
//        String documentType = "ID Card";
//        String standardizedDocumentType = standardize(FieldType.DOCUMENT_TYPE, documentType, DataSource.MEMBER_WORLD);
//        System.out.println("原始证件类型：" + documentType + "，标准化后：" + standardizedDocumentType);
//
////        String documentNum = "1eBID1bijQ4/14Y+NDQBbw==";
////        String standardizeddocumentNum = standardize(FieldType.DOCUMENT_NUM, documentNum);
////        System.out.println("原始证件号码：" + documentNum + "，标准化后：" + standardizeddocumentNum);
//
//        String ethnicGroup = "汉族";
//        String standardizedEthnicGroup = standardize(FieldType.ETHNIC_GROUP, ethnicGroup);
//        System.out.println("原始民族：" + ethnicGroup + "，标准化后：" + standardizedEthnicGroup);
//
//        String province = "北京市";
//        String standardizedProvince = standardize(FieldType.PROVINCE, province);
//        System.out.println("原始省份：" + province + "，标准化后：" + standardizedProvince);
//
//        String gender = "男";
//        String standardizedGender = standardize(FieldType.GENDER, gender);
//        System.out.println("原始性别：" + gender + "，标准化后：" + standardizedGender);
//
//        String nationality = "中国";
//        String standardizedNationality = standardize(FieldType.NATIONALITY, nationality);
//        System.out.println("原始国籍：" + nationality + "，标准化后：" + standardizedNationality);
//
//        String gender1 = "dddd";
//        String standardizedgender = standardize(FieldType.GENDER, gender1);
//        System.out.println("原始性别：" + gender1 + "，标准化后：" + standardizedgender);

        String ffp= "J4uQkl/qoMwqDpVu8llhpQ==";
        ffp = NormalizationUtils.standardize(FieldType.FREQUENT_FLYER_NUM, ffp, DataSource.LUYAN_STEWARD);
        System.out.println("FFP：" + ffp + "，标准化后：" + ffp);

    }

    public static String removeNameSuffix(String name) {
        String regex = "\\s*(MR|MS|GM|JC|VIP|VVIP|CIP|CHD|CHD\\s*\\([^)]*\\))\\s*$";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(name);

        if (matcher.find()) {
            return matcher.replaceFirst("");
        } else {
            return name;
        }
    }

    private static String removeInfantSuffix(String name) {
//        String MONTH_ABBREVIATIONS = "(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)";
        String regex = "\\s*INF\\([^)]*\\)\\s*$";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(name);

        if (matcher.find()) {
            return matcher.replaceFirst("");
        } else {
            return name;
        }
    }

}
