package com.travelsky.dataplatform.udf;


public class IDCardUDF {


    /**
     *
     * @param keyStr 获取的字段
     * @param idCard 身份证号
     * @return String
     */
    public String evaluate(String keyStr, String idCard) throws Exception {
        if (idCard == null || keyStr == null) {
            return null;
        }
        // 基本格式验证：15位或18位
        if (idCard.length() != 15 && idCard.length() != 18) {
            throw new Exception("身份证长度异常： " + idCard);
        }
        String value = "";
        if ("SEX".equals(keyStr)) {
            value = getGender(idCard);
        } else if ("BIRTHDAY".equals(keyStr)) {
            value = getGender(idCard);
        }
        return value;
    }


    public static String getBirthday(String idCard) {
        if (idCard.length() == 15) {
            return "19" + idCard.substring(6, 8) + "-" + idCard.substring(8, 10) + "-" + idCard.substring(10, 12) + " 00:00:00.000";
        } else if (idCard.length() == 18) {
            return idCard.substring(6, 10) + "-" + idCard.substring(10, 12) + "-" + idCard.substring(12, 14) + " 00:00:00.000";
        } else {
            return null;
        }
    }

    public static String getGender(String idCard) {
        if (idCard.length() == 15) {
            return Integer.parseInt(idCard.substring(14, 15)) % 2 == 0 ? "F" : "M";
        } else if (idCard.length() == 18) {
            return Integer.parseInt(idCard.substring(16, 17)) % 2 == 0 ? "F" : "M";
        } else {
            return null;
        }
    }

}
