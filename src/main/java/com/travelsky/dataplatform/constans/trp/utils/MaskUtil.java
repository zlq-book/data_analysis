package com.travelsky.dataplatform.constans.trp.utils;

/**
 * 掩码工具栏
 */
public class MaskUtil {

    /**
     * 掩码：保留前2位和后2位
     * 长度小于等于4，全展示
     * 长度大于4，保留前2位和后2位
     */
    public static String maskFront2Back2(String cardNumber) {
        if (cardNumber == null || cardNumber.isEmpty()) {
            return cardNumber;
        }

        int length = cardNumber.length();
        if (length <= 4) {
            return cardNumber;
        }

        String prefix = cardNumber.substring(0, 2);
        String suffix = cardNumber.substring(length - 2);
        return prefix + "***" + suffix;
    }

    /**
     * 手机号掩码：展示前3位和后4位，中间用 * 替代，如： 138****1234
     */
    public static String maskPhoneFront3Back4(String phone) {
        if (phone == null || phone.isEmpty()) {
            return phone;
        }

        int length = phone.length();
        if (length <= 7) {
            return phone; // 不够展示前3+后4
        }

        String prefix = phone.substring(0, 3);
        String suffix = phone.substring(length - 4);
        int middleLength = length - 3 - 4;
        StringBuilder maskedMiddle = new StringBuilder();
        for (int i = 0; i < middleLength; i++) {
            maskedMiddle.append("*");
        }
        return prefix + maskedMiddle + suffix;
    }

    /**
     * 掩码：只展示后4位，其余用 * 替代
     */
    public static String maskLast4(String cardNumber) {
        if (cardNumber == null || cardNumber.isEmpty()) {
            return cardNumber;
        }

        int length = cardNumber.length();
        if (length <= 4) {
            return cardNumber;
        }

        int maskedLength = length - 4;
        StringBuilder maskedPart = new StringBuilder();
        for (int i = 0; i < maskedLength; i++) {
            maskedPart.append("*");
        }
        String visiblePart = cardNumber.substring(length - 4);
        return maskedPart.append(visiblePart).toString();
    }

    /**
     * 掩码邮箱：展示邮箱用户名第1位和最后1位，中间用 * 掩码，域名不变
     * 示例：z****e@gmail.com
     */
    public static String maskEmail(String email) {
        if (email == null || email.isEmpty()) {
            return email;
        }

        int atIndex = email.indexOf('@');
        if (atIndex < 2) {
            return email; // 不是有效邮箱格式
        }

        String username = email.substring(0, atIndex);
        String domain = email.substring(atIndex); // 包括 @

        int usernameLen = username.length();
        if (usernameLen <= 1) {
            return "*" + domain;
        }

        char firstChar = username.charAt(0);
        char lastChar = username.charAt(usernameLen - 1);

        StringBuilder maskedUsername = new StringBuilder();
        maskedUsername.append(firstChar);
        for (int i = 0; i < usernameLen - 2; i++) {
            maskedUsername.append("*");
        }
        maskedUsername.append(lastChar);

        return maskedUsername + domain;
    }
}
