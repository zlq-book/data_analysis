package com.travelsky.dataplatform.utils;

/**
 * @author kuangaihua
 * @date 2025/8/11 15:49
 */
public class IdCardUtils {
    public static String getBirthday(String idCard) {
        if (idCard.length() == 15) {
            return "19" + idCard.substring(6, 8) + "-" + idCard.substring(8, 10) + "-" + idCard.substring(10, 12)+" 00:00:00.000";
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
