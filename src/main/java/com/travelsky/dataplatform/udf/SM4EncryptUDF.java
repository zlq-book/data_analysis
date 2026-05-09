package com.travelsky.dataplatform.udf;

import com.travelsky.dataplatform.utils.SM4Utils;
import org.apache.flink.table.functions.ScalarFunction;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.security.Security;
import java.util.Base64;

/**
 * @author kuangaihua
 * @date 2025/7/1 16:11
 */
public class SM4EncryptUDF extends ScalarFunction {
    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    private static final String TRANSFORMATIONECB = "SM4/ECB/PKCS5Padding";
    private static final int KEY_LENGTH = 16;

    private static byte[] validateKey(String keyStr) {
        byte[] keyByte = Base64.getDecoder().decode(keyStr);
        if (keyByte.length != KEY_LENGTH) {
            throw new IllegalArgumentException("密钥必须为16个字符");
        }
        return keyByte;
    }

    public String eval(String plaintext, String keyStr) {
        String encrypt = SM4Utils.encrypt(plaintext, keyStr);
        return encrypt;
    }
}
