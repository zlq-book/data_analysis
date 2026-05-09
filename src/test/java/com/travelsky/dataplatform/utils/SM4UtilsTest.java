package com.travelsky.dataplatform.utils;

import com.travelsky.dataplatform.constans.Constants;

import javax.xml.bind.DatatypeConverter;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;

/**
 * @author kuangaihua
 * @date 2025/8/28 18:34
 */
public class SM4UtilsTest {
    public static void main(String[] args) {
        test1();
    }


    public static void test1() {
        String encrypt = SM4Utils.encrypt("2", SM4Utils.hexToBase64("1fd3593d5d1a1951a1dc36cd5cb8d01c"));
        String decrypt = SM4Utils.decrypt(encrypt, SM4Utils.hexToBase64("1fd3593d5d1a1951a1dc36cd5cb8d01c"));
        System.out.println(encrypt);
        System.out.println(decrypt);
    }

    public static void test2() {
        for (int i = 0; i < 1000000; i++) {
            String decrypt = SM4Utils.encrypt("3GUOGQGER5KW", Constants.SM4_KEY);
        }
    }

    public static void encriptValue() {
        String paintxt = "TongyikehuA1~";
        String key = SM4Utils.hexToBase64("1fd3593d5d1a1951a1dc36cd5cb8d01c");
        String encValue = SM4Utils.base64ToHex(SM4Utils.encrypt(paintxt, key));
        System.out.println("encValue=" + encValue);
    }

    public static void plaintext2base642hex() {
        String key = "plaintext";
        String hexstr = DatatypeConverter.printHexBinary(key.getBytes(StandardCharsets.UTF_8));
        System.out.println("hexstr=" + hexstr);
    }
}
