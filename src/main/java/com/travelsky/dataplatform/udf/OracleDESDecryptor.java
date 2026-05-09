package com.travelsky.dataplatform.udf;

import com.travelsky.dataplatform.constans.Constants;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import java.nio.charset.StandardCharsets;

/**
 * 解密工具类
 * 加密方式：
 * 密钥位数: DES
 * 连接方式: ECB
 * 填充方式: PKCS5Padding
 */
public class OracleDESDecryptor extends ScalarFunction {
    static final Logger logger = LoggerFactory.getLogger(OracleDESDecryptor.class);
    /**
     * DES解密函数 (8字节密钥)
     * @param encryptedStr 输入密文字符串（16进制字符串）
     * @param key 密钥字符串（8字节）
     * @return 明文字符串
     * @throws Exception 解密过程中的异常
     */
    public static String eval(String encryptedStr, String key) {

        try {
            // 1. 检查参数
            if (encryptedStr == null) {
                return null;
            }
            if (key == null || key.length() != 16) {
                logger.error("Hex密钥必须是16个字符{},{}", key, encryptedStr);
                System.out.println("Hex密钥必须是16个字符key:" + key + ",encryptedHex:" + encryptedStr);
                return encryptedStr;
            }
            // 将16进制字符串转换为字节数组
            byte[] encryptedData = hexStringToByteArray(encryptedStr);

            // 将密钥字符串转换为字节数组
            byte[] keyBytes = DatatypeConverter.parseHexBinary(key);

            if (keyBytes.length != 8) {
                throw new IllegalArgumentException("DES密钥必须是8字节长度");
            }

            // 创建DES密钥规范
            SecretKey secretKey = new SecretKeySpec(keyBytes, "DES");

            // 获取Cipher实例并初始化为解密模式
            Cipher cipher = Cipher.getInstance("DES/ECB/PKCS5Padding");
            cipher.init(Cipher.DECRYPT_MODE, secretKey);

            // 执行解密
            byte[] decryptedData = cipher.doFinal(encryptedData);

            // 将解密后的字节数组转换为字符串
            return new String(decryptedData, StandardCharsets.UTF_8).trim();
        } catch (Exception e) {
            logger.error("解密失败{},{}", encryptedStr, e);
            System.out.println("解密失败:" + encryptedStr);
            return encryptedStr;
        }

    }

    /**
     * 16进制字符串转换为字节数组
     * @param hexString 16进制字符串
     * @return 字节数组
     */
    private static byte[] hexStringToByteArray(String hexString) {
        if (hexString == null || hexString.length() % 2 != 0) {
            System.out.println("密文错误，请检查");
            return hexString.getBytes();
        }

        int len = hexString.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4)
                    + Character.digit(hexString.charAt(i+1), 16));
        }
        return data;
    }

    public static void main(String[] args) {
        System.out.println(eval("EBF49EE1477C80FE3A72BCCDE1B43441575364D2EB1B9A1A", Constants.LYGJ_ZB_AES_KEY));
    }


}
