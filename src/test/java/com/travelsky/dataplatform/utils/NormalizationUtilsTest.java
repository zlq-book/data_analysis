package com.travelsky.dataplatform.utils;

/**
 * @author kuangaihua
 * @date 2025/9/17 10:51
 */
public class NormalizationUtilsTest {
    public static void main(String[] args) {
        String credentialType="IDCARD";
        credentialType=NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, credentialType,DataSource.MEMBER_WORLD);
        System.out.println(credentialType);

        System.out.println(NormalizationUtils.standardize(FieldType.CN_NAME, "周枫柠 CHD(UM10)"));
    }
}
