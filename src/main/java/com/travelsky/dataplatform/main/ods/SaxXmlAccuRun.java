package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.TOdsClkAccu;
import com.travelsky.dataplatform.source.XmlFileSourceAccu;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.KerberosAuthUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.sql.Date;
import java.time.LocalDate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SaxXmlAccuRun {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
           return;
        }
        String etlDate = args[0];

//        String etlDate = "2024-12-02";
        if (StringUtils.isBlank(etlDate)) {
            // 日期空结束
           return;
        }
        String fileName = "";
        String destFileName = "";
        Pattern pattern = Pattern.compile("([0-9]{4})-([0-9]{2})-[0-9]{2}");
        Matcher matcher = pattern.matcher(etlDate);
        if (matcher.find()) {
            fileName = "ACCU" + matcher.group(1) + matcher.group(2) + ".xml";
            destFileName = "ACCU" + matcher.group(1) + matcher.group(2) + "done.xml";
        } else {
            return;
        }
        String path = Constants.CLK_FILE_PATH + "/" + fileName;
        Configuration conf = new Configuration();
        try {
            conf.set("fs.defaultFS", "hdfs://cmss");
            conf.set("dfs.nameservices", "cmss");
            conf.set("dfs.ha.namenodes.cmss", "nn1,nn2");
            conf.set("dfs.namenode.rpc-address.cmss.nn1", "emr-master-001.novalocal:8020");
            conf.set("dfs.namenode.rpc-address.cmss.nn2", "emr-master-ha-001.novalocal:8020");
            conf.set("dfs.client.failover.proxy.provider.cmss",
                    "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
//            KerberosAuthUtils.initKerberosAuth(Constants.KRB5_CONF_PATH, Constants.SYS_UCV_TEST_FOR_MAPREDUCE_PRINCIPAL, Constants.SYS_UCV_TEST_FOR_MAPREDUCE_KEYTAB_PATH);


        } catch (Exception e) {
            e.printStackTrace();
        }
        FileSystem fs = FileSystem.get(conf);

        // hdfs文件是否存在
        boolean exists = fs.exists(new Path(path));
        if (!exists) {
            fs.close();
            // 文件不存在 结束
            return;
        }
        fs.close();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "SaxXmlAccuRun");
        env.setParallelism(1);
        DataStream<TOdsClkAccu> fileDataStreamSource = env.addSource(
                new XmlFileSourceAccu(path, etlDate));


        //创建dorisSink
        DorisSink<TOdsClkAccu> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.ODS_DB,
                "T_ODS_CLK_ACCU",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.ODS_USER,
                Constants.ODS_PWD);
        //数据写入doris
        fileDataStreamSource.sinkTo(dorisSink);
        fileDataStreamSource.print();
        env.execute("SaxXmlAccuRun");
        // 处理完成 重命名
        fs = FileSystem.get(conf);
        fs.rename(new Path(path), new Path(Constants.CLK_FILE_PATH + "/" + destFileName));
        fs.close();

    }
}
