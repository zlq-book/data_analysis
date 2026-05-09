package com.travelsky.dataplatform.constans.trp.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

/**
 * @author zcl
 * document和String的转换
 */
public class DocumentUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentUtils.class);
    private static final DocumentBuilderFactory FACTORY = DocumentBuilderFactory.newInstance();

    public static Document string2Document(String xml) throws ParserConfigurationException, IOException, SAXException {
        StringReader stringReader = new StringReader(xml);
        InputSource is = new InputSource(stringReader);
        DocumentBuilder builder = FACTORY.newDocumentBuilder();
        Document doc = builder.parse(is);
        stringReader.close();
        return doc;
    }

    /*
     * 把dom文件转换为xml字符串
     */
    public static String toStringFromDoc(Document document) {
        String result = null;
        StringWriter strWtr = null;
        StreamResult strResult = null;
        try {
            if (document != null) {
                strWtr = new StringWriter();
                strResult = new StreamResult(strWtr);

                TransformerFactory factory = TransformerFactory.newInstance();
                factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
                Transformer transformer = factory.newTransformer();
                transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
                transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                transformer.setOutputProperty(OutputKeys.METHOD, "xml"); // xml, html,
                // text
                transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
                transformer.transform(new DOMSource(document.getDocumentElement()), strResult);


                result = strResult.getWriter().toString().replace("<?xml version=\"1.0\" encoding=\"UTF-8\"?>", "");
            }
        } catch (TransformerConfigurationException e) {
            LOGGER.error(e.getMessage(), e);
        } catch (TransformerException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {

            try {
                strWtr.close();
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            }


        }

        return result;
    }

    /**
     *将node对象转换成Document
     * @param node
     * @return
     * @throws ParserConfigurationException
     */
    public static Document nodeToDocument(Node node) throws ParserConfigurationException {
        FACTORY.setNamespaceAware(true);
        DocumentBuilder builder = FACTORY.newDocumentBuilder();
        Document newDocument = builder.newDocument();
        Node importedNode = newDocument.importNode(node, true);
        newDocument.appendChild(importedNode);
        return newDocument;
    }

}
