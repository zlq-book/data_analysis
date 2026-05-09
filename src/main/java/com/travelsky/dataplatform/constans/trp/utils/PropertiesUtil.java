package com.travelsky.dataplatform.constans.trp.utils;

import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.Properties;

public class PropertiesUtil {
    private Properties pps;
    final static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PropertiesUtil.class);

    private static PropertiesUtil instance;
    public PropertiesUtil(){};

    public static PropertiesUtil getInstance(){
        if(instance == null){
            instance = new PropertiesUtil();
        }
        return instance;
    }

    /**
     * 通过key取得value
     *
     * @param key
     * @return
     */
    public String getValueByKey(String key)  {
        //PropertiesUtil propertiesUtil = new PropertiesUtil();
        String value;
        if (pps == null) {
            LOGGER.info("getValueByKey  pps == null");
            try {
                getAllProperties("/system.properties");
            } catch (IOException e) {
                LOGGER.error(String.valueOf(e));
            }
        }
        value = pps.getProperty(key);

        LOGGER.info("getValueByKey  key=" + key + ",value=[" + value + "]");
        return value;
    }

    /**
     * 读取Properties的全部信息
     *
     * @param filePath
     * @throws IOException
     */
    public void getAllProperties(String filePath) throws IOException {

        LOGGER.info("getAllProperties 参数 filePath ："+filePath);
        pps = new Properties();
        //设定为Resource文件夹
        InputStream in = this.getClass().getResourceAsStream(filePath);
        LOGGER.info("getAllProperties pps 的值"+String.valueOf(pps));
        if (pps == null){
            LOGGER.info("getAllProperties pps 值为 null");
        }
        LOGGER.info("getAllProperties in 的值"+String.valueOf(in));
        if (in == null){
            LOGGER.info("getAllProperties in 值为 null");
        }
        if(pps != null){
            pps.load(in);
            //得到配置文件的名字
            Enumeration en = pps.propertyNames();

            while (en.hasMoreElements()) {
                String strKey = (String) en.nextElement();
                String strValue = pps.getProperty(strKey);
                LOGGER.info(strKey + "=" + strValue);
            }
        }
        try {
            if (in != null){
                in.close();
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage(),e);
        }
    }


    /**
     * 写入Properties信息
     *
     * @param pKey
     * @param pValue
     * @throws IOException
     */
    public void writeProperties(String pKey, String pValue) throws IOException {
        Properties pps = new Properties();
        //设定为当前文件夹
        File directory = new File("");
        //获取绝对路径
        String absolutePath = directory.getAbsolutePath();
        String filePath = "/src/main/resources/system.properties";
        InputStream in = new FileInputStream(absolutePath + filePath);
        //从输入流中读取属性列表（键和元素对）
        pps.load(in);
        //调用 Hashtable 的方法 put。使用 getProperty 方法提供并行性。
        //强制要求为属性的键和值使用字符串。返回值是 Hashtable 调用 put 的结果。
        OutputStream out = new FileOutputStream(absolutePath + filePath);
        pps.setProperty(pKey, pValue);
        //以适合使用 load 方法加载到 Properties 表中的格式，
        //将此 Properties 表中的属性列表（键和元素对）写入输出流
        pps.store(out, "Update " + pKey + " name");
        in.close();
        out.close();
    }
}
