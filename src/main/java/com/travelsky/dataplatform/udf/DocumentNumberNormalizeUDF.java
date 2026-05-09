package com.travelsky.dataplatform.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * 证件号标准化UDF
 * 对证件号明文进行标准化处理：去除首尾空格并转换为大写
 * 注意：此UDF只做标准化处理，不涉及加解密
 */
public class DocumentNumberNormalizeUDF extends ScalarFunction {
    
    /**
     * 证件号标准化处理
     * @param documentNumber 证件号明文
     * @return 标准化后的证件号（去除首尾空格并转换为大写），如果输入为null则返回null
     */
    public String eval(String documentNumber) {
        if (documentNumber == null) {
            return null;
        }
        return documentNumber.trim().toUpperCase();
    }
}

