package com.travelsky.trp.usercenter.data.analysis.cascrmclient;

import com.travelsky.dataplatform.constans.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.soap.SOAPEnvelope;
import javax.xml.soap.SOAPHeader;
import javax.xml.soap.SOAPMessage;
import javax.xml.soap.SOAPPart;
import javax.xml.ws.handler.MessageContext;
import javax.xml.ws.handler.soap.SOAPHandler;
import javax.xml.ws.handler.soap.SOAPMessageContext;
import java.io.ByteArrayInputStream;
import java.util.Set;

public class SecurityHandler implements SOAPHandler<SOAPMessageContext> {
    static final Logger logger = LoggerFactory.getLogger(SecurityHandler.class);
    @Override
    public boolean handleMessage(SOAPMessageContext context) {
        try {
            Boolean outboundProperty = (Boolean) context.get(SOAPMessageContext.MESSAGE_OUTBOUND_PROPERTY);

            if (outboundProperty) {
                SOAPMessage soapMessage = context.getMessage();

                // 直接操作SOAP消息的XML内容
                String headerXml =
                        "<soapenv:Header xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\">" +
                                "<customer>" +
                                "<name>" + Constants.CA_CRM_INTERFACE + "</name>" +
                                "<password>" +Constants.CA_CRM_KEY + "</password>" +
                                "</customer>" +
                                "</soapenv:Header>";

                // 移除现有的Header
                SOAPPart soapPart = soapMessage.getSOAPPart();
                SOAPEnvelope envelope = soapPart.getEnvelope();
                if (envelope.getHeader() != null) {
                    envelope.getHeader().detachNode();
                }

                // 添加新的Header
                SOAPHeader header = envelope.addHeader();

                // 创建DocumentBuilder来解析XML
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                factory.setNamespaceAware(true);
                DocumentBuilder builder = factory.newDocumentBuilder();
                Document document = builder.parse(new ByteArrayInputStream(headerXml.getBytes()));

                // 导入节点到SOAP消息
                Node importedNode = soapPart.importNode(document.getDocumentElement(), true);
                header.appendChild(importedNode);

                soapMessage.saveChanges();

                // 调试输出
                //System.out.println("=== SOAP请求消息 ===");
                //soapMessage.writeTo(System.out);
                //System.out.println("\n===================");
            }

            return true;
        } catch (Exception e) {
            logger.error("SOAP Handler处理失败", e);
            throw new RuntimeException("SOAP Handler处理失败", e);
        }
    }

    @Override
    public boolean handleFault(SOAPMessageContext context) {
        return true;
    }

    @Override
    public void close(MessageContext context) {
    }

    @Override
    public Set<QName> getHeaders() {
        return null;
    }
}
