package com.travelsky.dataplatform.utils;

/**
 * @author kuangaihua
 * @date 2025/8/11 15:52
 */
public class IdCardUtilsTest {
    public static void main(String[] args) {

        getBirthdayTest();
        getGenderTest();
    }
    public static void getBirthdayTest() {
        String birthday = IdCardUtils.getBirthday("420826198901014818");
        System.out.println(birthday);
    }
    public static void getGenderTest() {
        String gender = IdCardUtils.getGender("420826198901014818");
        System.out.println(gender);
    }
}
