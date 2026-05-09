package com.travelsky.dataplatform.source;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.main.ods.SaxXmlTOdsClkAccuint;
import com.travelsky.dataplatform.module.ods.TOdsClkAccu;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.InputStream;

public class XmlFileSourceAccuint extends RichParallelSourceFunction<TOdsClkAccu> {
    static final Logger logger = LoggerFactory.getLogger(XmlFileSourceAccuint.class);

    // 输入流
    private String fileName;
    private String etlDate;

    private transient XMLReader xmlReader;
    private transient InputStream inputStream;
    private transient FileSystem fs;
    private volatile boolean isRunning = true;

    public XmlFileSourceAccuint(String fileName, String etlDate) {
        this.fileName = fileName;
        this.etlDate = etlDate;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        try {

            System.setProperty("java.security.krb5.conf", Constants.KRB5_CONF_PATH);
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            conf.set("fs.defaultFS", "hdfs://cmss");
            conf.set("dfs.nameservices", "cmss");
            conf.set("dfs.ha.namenodes.cmss", "nn1,nn2");
            conf.set("dfs.namenode.rpc-address.cmss.nn1", "emr-master-001.novalocal:8020");
            conf.set("dfs.namenode.rpc-address.cmss.nn2", "emr-master-ha-001.novalocal:8020");
            conf.set("dfs.client.failover.proxy.provider.cmss",
                    "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
            fs = FileSystem.get(conf);

            // 1. 在 open() 中打开流（TaskManager 本地执行）
            this.inputStream = fs.open(new Path( fileName)); // 或从 HDFS、S3 等获取流

            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser saxParser = factory.newSAXParser();
            // 2. 创建 XMLReader
            this.xmlReader = saxParser.getXMLReader();

        } catch (Exception e) {
            e.printStackTrace();
        }


    }


    @Override
    public void run(SourceContext<TOdsClkAccu> ctx) throws Exception {
        SaxXmlTOdsClkAccuint handler = new SaxXmlTOdsClkAccuint(etlDate);
        xmlReader.setContentHandler(handler);
        try {
            InputSource is = new InputSource(inputStream);
            xmlReader.parse(is);
        } catch (IOException | SAXException e) {
            throw new RuntimeException("Error parsing XML file: ", e);
        }

        for (TOdsClkAccu dto : handler.getDtoList()) {
            ctx.collect(dto);
        }
    }

    @Override
    public void cancel() {

    }

    @Override
    public void close() throws Exception {
        super.close();
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (fs != null) {
            try {
                fs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
