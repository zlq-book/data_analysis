package com.travelsky.dataplatform.udf;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.trp.usercenter.data.analysis.transform.hsd.SplitFactTrans;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import java.nio.charset.StandardCharsets;


public class OracleAESDecryptor extends ScalarFunction {
    static final Logger logger = LoggerFactory.getLogger(OracleAESDecryptor.class);
    /**
     * 模拟 Oracle AES_DECRYPT 函数的 Java 实现
     * @param encryptedHex 加密后的 HEX 字符串
     * @param key 16字节的密钥字符串
     * @return 解密后的明文
     */
    public static String eval(String encryptedHex, String key) {
        try {
            // 1. 检查参数
            if (encryptedHex == null) {
                return null;
            }
            if (key == null || key.length() != 32) {
                logger.error("Hex密钥必须是32个字符{},{}", key, encryptedHex);
                System.out.println("Hex密钥必须是32个字符key:" + key + ",encryptedHex:" + encryptedHex);
                return encryptedHex;
            }

            // 2. 准备密钥和加密数据
            byte[] keyBytes = DatatypeConverter.parseHexBinary(key);
            byte[] encryptedData = hexStringToByteArray(encryptedHex);

            // 3. Oracle 默认使用全零的 IV (初始化向量)
            byte[] iv = new byte[16]; // 全零 IV
            IvParameterSpec ivSpec = new IvParameterSpec(iv);

            // 4. 初始化 AES/CBC/PKCS5Padding 解密器
            SecretKeySpec secretKey = new SecretKeySpec(keyBytes, "AES");
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.DECRYPT_MODE, secretKey, ivSpec);

            // 5. 解密
            byte[] decryptedBytes = cipher.doFinal(encryptedData);
            return new String(decryptedBytes, StandardCharsets.UTF_8).trim();
        } catch (Exception e) {
            logger.error("解密失败{},{}", encryptedHex, e);
            System.out.println("解密失败:" + encryptedHex);
            return encryptedHex;
        }
    }

    /**
     * HEX 字符串转字节数组 (兼容 Java 8)
     */
    private static byte[] hexStringToByteArray(String hex) {
        if (hex == null || hex.length() % 2 != 0) {
            logger.error("Hex 字符串长度必须为偶数{},{}", hex);
            System.out.println("Hex 字符串长度必须为偶数:" + hex);
            // 返回原始字符串
            return hex.getBytes(StandardCharsets.UTF_8);
        }

        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            int high = Character.digit(hex.charAt(i), 16);
            int low = Character.digit(hex.charAt(i + 1), 16);

            if (high == -1 || low == -1) {
                return hex.getBytes(StandardCharsets.UTF_8);
            }

            data[i / 2] = (byte) ((high << 4) + low);
        }
        return data;
    }

    // 示例用法
    public static void main(String[] args) {
        String key = ""; // 16字节密钥
        String originalText = "";

        String encrypted = eval(originalText, Constants.CLK_AES_KEY);
        System.out.println("解密后: " + encrypted);

    }
}
