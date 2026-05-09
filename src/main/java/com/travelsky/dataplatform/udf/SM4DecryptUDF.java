package com.travelsky.dataplatform.udf;

import com.travelsky.dataplatform.constans.Constants;
import org.apache.flink.table.functions.ScalarFunction;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;
import java.security.Security;

public class SM4DecryptUDF extends ScalarFunction {
    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    private static final String TRANSFORMATION_ECB = "SM4/ECB/PKCS5Padding";
    private static final String ALGORITHM = "SM4";
    private static final int KEY_LENGTH = 16;

    private static byte[] validateKey(String keyStr) {
        byte[] keyByte = Base64.getDecoder().decode(keyStr);
        if (keyByte.length != KEY_LENGTH) {
            System.out.println("密钥长度错误");
            throw new IllegalArgumentException("密钥必须为16字节（Base64解码后）");
        }
        return keyByte;
    }

    public String eval(String ciphertext, String keyStr) {
        if (ciphertext == null || keyStr == null) {
            return null;
        }

        try {
            return decrypt(ciphertext, keyStr);
        } catch (Exception e) {
            System.out.println("SM4解密失败: " + e.getMessage());
           return ciphertext;
        }
    }

    public static String decrypt(String ciphertext, String keyStr) {
        try {
            byte[] keyBytes = validateKey(keyStr);
            byte[] ciphertextBytes = Base64.getDecoder().decode(ciphertext);

            SecretKeySpec secretKey = new SecretKeySpec(keyBytes, ALGORITHM);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION_ECB, "BC");
            cipher.init(Cipher.DECRYPT_MODE, secretKey);

            byte[] decryptedBytes = cipher.doFinal(ciphertextBytes);
            return new String(decryptedBytes, "UTF-8");

        } catch (Exception e) {
            System.out.println("SM4解密失败: " + e.getMessage());
            throw new RuntimeException("SM4解密失败: " + e.getMessage(), e);
        }
    }

    public static void main(String[] args) {
        String ciphertext = "J6Rfarg+SZQtb6Jrk39BgA==";
        System.out.println(decrypt(ciphertext, Constants.SM4_KEY));
    }

}
