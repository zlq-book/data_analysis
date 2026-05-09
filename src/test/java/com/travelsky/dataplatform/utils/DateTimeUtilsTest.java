package com.travelsky.dataplatform.utils;

import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;

/**
 * @author kuangaihua
 * @date 2025/8/5 11:18
 */
public class DateTimeUtilsTest {
    public static void main(String[] args) {

        dateTimeFormatTest();
    }
    public static void dateTimeFormatTest() {
        String s = DateTimeUtils.dateFormatToDateTime("20250805", "yyyyMMdd");
        System.out.println(s);
    }
}
