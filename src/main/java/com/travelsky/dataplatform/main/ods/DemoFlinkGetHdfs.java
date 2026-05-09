package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.KerberosAuthUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * @author kuangaihua
 * @date 2025/6/27 15:39
 */
public class DemoFlinkGetHdfs {
    public static void main(String[] args) throws Exception {
// 1. 配置Kerberos认证
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://cmss");
        conf.set("dfs.nameservices", "cmss");
        conf.set("dfs.ha.namenodes.cmss", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.cmss.nn1", "emr-master-001.novalocal:8020");
        conf.set("dfs.namenode.rpc-address.cmss.nn2", "emr-master-ha-001.novalocal:8020");
        conf.set("dfs.client.failover.proxy.provider.cmss",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
/*      System.setProperty("java.security.krb5.conf", Constants.KRB5_CONF_PATH);
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hadoop.security.authorization", "true");
        conf.set("hadoop.security.token.renewer.master.interval", "60*60*23");
        conf.set("hadoop.security.token.renewer.master.timeout", "300");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(Constants.SYS_UCV_TEST_FOR_MAPREDUCE_PRINCIPAL, Constants.SYS_UCV_TEST_FOR_MAPREDUCE_KEYTAB_PATH);
*/

        // 2. 创建Flink环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "DemoFlinkGetHdfs");
        // 3. 配置Hadoop Job
        Job job = Job.getInstance(conf);
        TextInputFormat.addInputPath(job, new Path("hdfs://cmss/data/test/encrypt/create_grafana_emr.sql"));

        // 4. 创建Hadoop输入格式
        HadoopInputFormat<LongWritable, Text> hadoopInputFormat =
                new HadoopInputFormat<>(new TextInputFormat(), LongWritable.class, Text.class, job);

        // 5. 读取HDFS文件
        DataStream<Tuple2<LongWritable, Text>> hdfsStream =
                env.createInput(hadoopInputFormat);
        // 6. 数据处理示例
        DataStream<String> processedStream = hdfsStream
                .map(new MapFunction<Tuple2<LongWritable, Text>, String>() {
                    @Override
                    public String map(Tuple2<LongWritable, Text> value) {
                        return "Line " + value.f0 + ": " + value.f1.toString();
                    }
                });

        // 7. 输出结果
        processedStream.print();

        // 8. 执行任务
        env.execute("Secure HDFS Reader");

    }

}
