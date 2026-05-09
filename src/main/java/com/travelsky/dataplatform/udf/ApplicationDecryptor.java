package com.travelsky.dataplatform.udf;

import com.travelsky.dataplatform.constans.Constants;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.ScalarFunction;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import java.security.SecureRandom;


public class ApplicationDecryptor extends ScalarFunction {
    private static final String ALGORITHM = "AES";
    private static final int keysize = 128;
    /**
     * 解密
     *
     * @param content 待解密内容
     * @return
     */
    public static String eval(String content, String pwd) {
        if (StringUtils.isBlank(content)) {
            return null;
        }
        try {
            byte[] enCodeFormat = getSecretKey(pwd);
            SecretKeySpec key = new SecretKeySpec(enCodeFormat, ALGORITHM);
            Cipher cipher = Cipher.getInstance(ALGORITHM);// 创建密码器
            cipher.init(Cipher.DECRYPT_MODE, key);// 初始化
            byte[] decryptFrom = DatatypeConverter.parseHexBinary(content);
            byte[] result = cipher.doFinal(decryptFrom);
            return new String(result); // 加密
        } catch (Exception e) {
            e.printStackTrace();
        }
        return content;
    }
    public static byte[] getSecretKey(String pwd) throws Exception {
        KeyGenerator kgen = KeyGenerator.getInstance(ALGORITHM);
        SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG");
        secureRandom.setSeed(DatatypeConverter.parseHexBinary(pwd));
        kgen.init(keysize, secureRandom);
        SecretKey secretKey = kgen.generateKey();
        return secretKey.getEncoded();
    }

    public static void main(String[] args) {
        System.out.println(eval("D844266F3FC3CAF09D1AA136D479391F8FC4EC5DC159FBD89F3E4E8953DD5B5F", Constants.DKHCL_AES_KEY_AIR_ORDER_PASSENGER));
        System.out.println(eval("5F6FA05F2AEED3FDDB4FADB2A53D8AB5", Constants.DKHCL_AES_KEY_CRM_EMPLOYEE));
        System.out.println(eval("49A01FEC8AA176F0EAEEDD3C9A61BD6D4D01C83B52AC23BFC4AF607B4DCE2968", Constants.DKHCL_AES_KEY_CRM_EMPLOYEE_CERT));
    }
}
