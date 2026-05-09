package com.travelsky.dataplatform.utils;

import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.Security;
import java.time.LocalDateTime;
import java.util.Base64;

/**
 * @author kuangaihua
 * @date 2025/7/1 16:41
 */
public class SM4Utils {
    private static final Logger logger = LoggerFactory.getLogger(SM4Utils.class);

    private static final String ALGORITHM = "SM4";
    private static final String TRANSFORMATIONCBC = "SM4/CBC/PKCS5Padding";
    private static final String TRANSFORMATIONECB = "SM4/ECB/PKCS5Padding";
    private static final int KEY_LENGTH = 16;
    //    private static final String IV = "JUzgwCrDIT6v4SMg+BMX4A==";
    private static int encryptNum = 0;

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    // 生成标准128位密钥
    public static String generateKey() throws Exception {
        KeyGenerator kg = KeyGenerator.getInstance("SM4", "BC");
        kg.init(128);
        byte[] bytes = kg.generateKey().getEncoded();
        String key = Base64.getEncoder().encodeToString(bytes);
        return key;

    }

    private static byte[] validateKey(String keyStr) {
        byte[] keyByte = Base64.getDecoder().decode(keyStr);
        if (keyByte.length != KEY_LENGTH) {
            throw new IllegalArgumentException("密钥必须为16个字符");
        }
        return keyByte;
    }

    //    base64转为16进制
    public static String base64ToHex(String base64EncodedString) {
        byte[] decodedBytes = Base64.getDecoder().decode(base64EncodedString);
        String hexString = new BigInteger(1, decodedBytes).toString(16);
        return hexString;
    }

    //    base64转为二进制
    public static String base64ToBin(String base64) {
        // 输入校验
        if (base64 == null || base64.trim().isEmpty()) {
            throw new IllegalArgumentException("Empty Base64 string");
        }
        if (!base64.matches("^[A-Za-z0-9+/]+={0,2}$")) {
            throw new IllegalArgumentException("Invalid Base64 format");
        }

        // Base64解码
        byte[] bytes = Base64.getDecoder().decode(base64);

        // 字节转二进制
        StringBuilder binaryStr = new StringBuilder();
        for (byte b : bytes) {
            binaryStr.append(String.format("%8s",
                    Integer.toBinaryString(b & 0xFF)).replace(' ', '0'));
        }
        return binaryStr.toString();
    }

    //    二进制转为base64
    public static String binToBase64(String binaryStr) {
        // 输入校验
        if (binaryStr == null || binaryStr.isEmpty()) {
            throw new IllegalArgumentException("Empty binary string");
        }
        if (!binaryStr.matches("^[01]+$")) {
            throw new IllegalArgumentException("Invalid binary string");
        }
        if (binaryStr.length() % 8 != 0) {
            throw new IllegalArgumentException("Binary length must be multiple of 8");
        }

        // 二进制转字节数组
        byte[] bytes = new byte[binaryStr.length() / 8];
        for (int i = 0; i < bytes.length; i++) {
            String byteStr = binaryStr.substring(i * 8, (i + 1) * 8);
            bytes[i] = (byte) Integer.parseInt(byteStr, 2);
        }

        // Base64编码
        return Base64.getEncoder().encodeToString(bytes);
    }

    public static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        return sb.toString();
    }

    //    base64转为16进制字符串
    public static String hexToBase64(String hexKey) {
// 1. 清洗输入（移除所有非HEX字符）
        String cleanHex = hexKey.replaceAll("[^0-9a-fA-F]", "");

        // 2. 格式校验
        if (cleanHex.length() % 2 != 0) {
            throw new IllegalArgumentException("Invalid HEX length:" +  hexKey + " must be even number");
        }
        if (!cleanHex.matches("^[0-9a-fA-F]+$")) {
            throw new IllegalArgumentException("Contains non-HEX characters");
        }

        // 3. HEX转字节数组
        byte[] bytes = new byte[cleanHex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            int index = i * 2;
            bytes[i] = (byte) Integer.parseInt(cleanHex.substring(index, index + 2), 16);
        }

        // 4. Base64编码
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static byte[] validateIV(String ivStr) {
        byte[] ivByte = Base64.getDecoder().decode(ivStr);
        if (ivByte.length != KEY_LENGTH) {
            throw new IllegalArgumentException("密钥必须为16个字符");
        }
        return ivByte;
    }

    /**
     * SM4加密+Base64编码
     *
     * @param plaintext 明文
     * @param keyStr    16字节密钥
     */
    public static String encrypt(String plaintext, String keyStr) {
        encryptNum++;
        if (encryptNum%100000==0){
            System.out.println(LocalDateTime.now().toString() + " 加密次数: " + encryptNum);
            logger.info(LocalDateTime.now().toString() + " 加密次数: " + encryptNum);
        }
        try {
            if (StringUtils.isBlank(plaintext)) {
                return null;
            }
            byte[] keyBytes = validateKey(keyStr);
            SecretKeySpec secretKey = new SecretKeySpec(keyBytes, "SM4");
            Cipher cipher = Cipher.getInstance(TRANSFORMATIONECB);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            byte[] encrypted = cipher.doFinal(plaintext.getBytes());
//            System.out.println(new String(encrypted));
            return Base64.getEncoder().encodeToString(encrypted);
        } catch (Exception e) {
            System.out.println(LocalDateTime.now().toString() + " 加密失败: " + e.getMessage());
            return plaintext;
        }
    }

    /**
     * Base64解码+SM4解密
     *
     * @param ciphertext Base64密文
     * @param keyStr     16字节密钥
     */
    public static String decrypt(String ciphertext, String keyStr) {
        if (ciphertext == null || ciphertext.trim().isEmpty()) {
            return null;
        }
        try {
            byte[] keyBytes = validateKey(keyStr);
            SecretKeySpec secretKey = new SecretKeySpec(keyBytes, "SM4");

            Cipher cipher = Cipher.getInstance(TRANSFORMATIONECB);
            cipher.init(Cipher.DECRYPT_MODE, secretKey);
            byte[] decrypted = Base64.getDecoder().decode(ciphertext);
            return new String(cipher.doFinal(decrypted), StandardCharsets.UTF_8);
        } catch (Exception e) {
            System.out.println("解密失败, ciphertext:"+ciphertext+ ", keyStr:"+ keyStr);
            e.printStackTrace();
            return ciphertext;
        }
    }

    /**
     * SM4加密（自动处理密钥和IV）
     *
     * @param plaintext 明文
     * @param keyStr    16字符密钥字符串
     * @param ivStr     16字符IV字符串
     */
    public static String encrypt(String plaintext, String keyStr, String ivStr) throws Exception {
        byte[] key = validateKey(keyStr);
        byte[] iv = validateIV(ivStr);

        Cipher cipher = Cipher.getInstance(TRANSFORMATIONCBC, "BC");
        cipher.init(Cipher.ENCRYPT_MODE,
                new SecretKeySpec(key, ALGORITHM),
                new IvParameterSpec(iv));

        byte[] encrypted = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(encrypted);
    }

    /**
     * SM4解密（自动处理密钥和IV）
     *
     * @param ciphertext Base64密文
     * @param keyStr     16字符密钥字符串
     * @param ivStr      16字符IV字符串
     */
    public static String decrypt(String ciphertext, String keyStr, String ivStr) throws Exception {
        byte[] key = validateKey(keyStr);
        byte[] iv = validateIV(ivStr);

        Cipher cipher = Cipher.getInstance(TRANSFORMATIONCBC, "BC");
        cipher.init(Cipher.DECRYPT_MODE,
                new SecretKeySpec(key, ALGORITHM),
                new IvParameterSpec(iv));

        byte[] decoded = Base64.getDecoder().decode(ciphertext);
        return new String(cipher.doFinal(decoded), StandardCharsets.UTF_8);
    }
}
