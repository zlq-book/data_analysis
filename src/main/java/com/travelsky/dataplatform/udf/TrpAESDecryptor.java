package com.travelsky.dataplatform.udf;

import com.openjaw.utils.crypto.CryptoAlgorithmAES;
import com.travelsky.dataplatform.constans.Constants;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TRP场景AES解密器
 * 处理Base64编码的AES加密数据
 * 使用OpenJaw CryptoAlgorithmAES进行解密，采用AES/ECB模式
 * 密钥为普通字符串格式
 */
public class TrpAESDecryptor extends ScalarFunction {
    static final Logger logger = LoggerFactory.getLogger(TrpAESDecryptor.class);
    
    /**
     * TRP默认AES密钥（从Constants中加载）
     */
    private static final String DEFAULT_TRP_AES_KEY = Constants.TRP_AES_KEY;

    /**
     * TRP AES解密函数 - 使用默认密钥（Flink UDF主方法）
     * @param encryptedBase64 Base64编码的加密字符串
     * @return 解密后的明文
     */
    public static String eval(String encryptedBase64) {
        return decrypt(encryptedBase64, DEFAULT_TRP_AES_KEY);
    }

    /**
     * TRP AES解密函数 - 自定义密钥（Flink UDF重载方法）
     * @param encryptedBase64 Base64编码的加密字符串
     * @param salt 密钥（盐值）字符串
     * @return 解密后的明文
     */
    public static String eval(String encryptedBase64, String salt) {
        return decrypt(encryptedBase64, salt);
    }
    
    /**
     * TRP AES解密函数 - 处理Base64编码的加密数据
     * 使用OpenJaw的CryptoAlgorithmAES进行解密，采用AES/ECB模式
     * @param encryptedBase64 Base64编码的加密字符串
     * @param salt 密钥（盐值）字符串
     * @return 解密后的明文
     */
    public static String decrypt(String encryptedBase64, String salt) {
        try {
            // 1. 检查参数
            if (encryptedBase64 == null) {
                return null;
            }
            if (salt == null || salt.isEmpty()) {
                logger.error("密钥不能为空, encryptedBase64={}", encryptedBase64);
                return encryptedBase64;
            }

            // 2. 使用OpenJaw CryptoAlgorithmAES进行解密（AES/ECB模式）
            CryptoAlgorithmAES cryptoAlgorithmAES = new CryptoAlgorithmAES(salt.getBytes(), "ECB");
            String decrypted = cryptoAlgorithmAES.decrypt(encryptedBase64);
            
            return decrypted != null ? decrypted.trim() : encryptedBase64;
        } catch (Exception e) {
            logger.error("解密失败, encryptedBase64={}, error={}", encryptedBase64, e.getMessage());
            return encryptedBase64;
        }
    }

    /**
     * TRP AES加密函数 - 使用默认密钥
     * @param data 明文数据
     * @return 加密后的Base64字符串
     */
    public static String encrypt(String data) {
        return encrypt(data, DEFAULT_TRP_AES_KEY);
    }

    /**
     * TRP AES加密函数 - 自定义密钥
     * @param data 明文数据
     * @param salt 密钥（盐值）字符串
     * @return 加密后的Base64字符串
     */
    public static String encrypt(String data, String salt) {
        try {
            if (data == null) {
                return null;
            }
            if (salt == null || salt.isEmpty()) {
                logger.error("密钥不能为空");
                return data;
            }
            
            CryptoAlgorithmAES cryptoAlgorithmAES = new CryptoAlgorithmAES(salt.getBytes(), "ECB");
            return cryptoAlgorithmAES.encrypt(data);
        } catch (Exception e) {
            logger.error("加密失败, data={}, error={}", data, e.getMessage());
            return data;
        }
    }

    // 测试示例
    public static void main(String[] args) {
        System.out.println("========== TRP AES加解密测试 ==========");
        System.out.println("默认密钥: " + DEFAULT_TRP_AES_KEY);
        
        // 测试用例1：使用默认密钥的完整加解密流程
        String originalText = "13800138000";
        String encrypted = encrypt(originalText);  // 使用默认密钥
        String decrypted = eval(encrypted);        // 使用默认密钥(Flink UDF)
        System.out.println("\n测试1 - 默认密钥加解密流程(UDF方法):");
        System.out.println("原文: " + originalText);
        System.out.println("加密后: " + encrypted);
        System.out.println("解密后: " + decrypted);
        System.out.println("验证: " + (originalText.equals(decrypted) ? "✓ 成功" : "✗ 失败"));
        
        // 测试用例2：已有的加密数据
        String encryptedBase64 = "MfkWro0xJF6eW5KnqKzduqXjQHCBa6+cGiyvJhdFaRY=";
        String decrypted2 = eval(encryptedBase64);  // 使用默认密钥
        System.out.println("\n测试2 - 已有加密数据(默认密钥):");
        System.out.println("加密数据: " + encryptedBase64);
        System.out.println("解密后: " + decrypted2);

        // 测试用例3：空值测试
        System.out.println("\n测试3 - 空值测试:");
        System.out.println("空值解密: " + eval(null));
        
        // 测试用例4：无效Base64测试
        System.out.println("\n测试4 - 无效Base64:");
        System.out.println("无效Base64测试: " + eval("invalid_base64!!!"));
        
        // 测试用例5：不同数据类型测试（默认密钥）
        System.out.println("\n测试5 - 不同数据类型(默认密钥):");
        String[] testData = {"张三", "zhangsan@example.com", "110101199001011234"};
        for (String data : testData) {
            String enc = encrypt(data);  // 使用默认密钥
            String dec = eval(enc);      // 使用默认密钥
            System.out.println("原文: " + data + " -> 加密: " + enc + " -> 解密: " + dec + " [" + (data.equals(dec) ? "✓" : "✗") + "]");
        }
        
        // 测试用例6：自定义密钥测试
        System.out.println("\n测试6 - 自定义密钥:");
        String customKey = "1234567890abcdef";
        String customEncrypted = encrypt("测试数据", customKey);
        String customDecrypted = eval(customEncrypted, customKey);  // 使用自定义密钥
        System.out.println("自定义密钥: " + customKey);
        System.out.println("加密后: " + customEncrypted);
        System.out.println("解密后: " + customDecrypted);
        System.out.println("验证: " + ("测试数据".equals(customDecrypted) ? "✓ 成功" : "✗ 失败"));
        
        // 测试用例7：两种方法对比
        System.out.println("\n测试7 - 方法对比:");
        String testText = "方法测试";
        String enc1 = encrypt(testText);
        String dec1 = eval(enc1);           // UDF方法
        String dec2 = decrypt(enc1, DEFAULT_TRP_AES_KEY);  // decrypt方法
        System.out.println("eval()方法: " + dec1 + " [" + (testText.equals(dec1) ? "✓" : "✗") + "]");
        System.out.println("decrypt()方法: " + dec2 + " [" + (testText.equals(dec2) ? "✓" : "✗") + "]");
        
        System.out.println("\n========== 测试完成 ==========");
    }
}
