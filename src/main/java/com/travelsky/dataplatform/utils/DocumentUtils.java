package com.travelsky.dataplatform.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;

/**
 * @author zcl
 * document和String的转换
 */
public class DocumentUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentUtils.class);
    private static final DocumentBuilderFactory FACTORY = DocumentBuilderFactory.newInstance();
    static {
        // 在静态初始化块中配置工厂
        FACTORY.setNamespaceAware(true);
    }

//    public static Document string2Document(String xml) {
//        InputSource is = new InputSource(new StringReader(xml));
//        DocumentBuilder builder;
//        try {
//            builder = factory.newDocumentBuilder();
//            Document doc = builder.parse(is);
//            return doc;
//        } catch (Exception e) {
//            LOGGER.info("String 2 document is error, the exception is {}", e.getMessage());
//        }
//        return null;
//    }

    public static Document string2Document(String xml) throws ParserConfigurationException, IOException, SAXException {
        InputSource is = new InputSource(new StringReader(xml));
        DocumentBuilder builder = FACTORY.newDocumentBuilder();
        return builder.parse(is);
    }
}
