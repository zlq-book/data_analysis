package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.KerberosAuthUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * @author kuangaihua
 * @date 2025/6/30 15:11
 */
public class DemoGetHdfsFile {
    public static void main(String[] args) {
        // 加载Kerberos配置和principal的keytab文件
        try {

//            System.setProperty("java.security.krb5.conf", Constants.KRB5_CONF_PATH);
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://cmss");
            conf.set("dfs.nameservices", "cmss");
            conf.set("dfs.ha.namenodes.cmss", "nn1,nn2");
            conf.set("dfs.namenode.rpc-address.cmss.nn1", "emr-master-001.novalocal:8020");
            conf.set("dfs.namenode.rpc-address.cmss.nn2", "emr-master-ha-001.novalocal:8020");
            conf.set("dfs.client.failover.proxy.provider.cmss",
                    "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
/*            conf.set("hadoop.security.authentication", "kerberos");
            conf.set("hadoop.security.authorization", "true");
            conf.set("hadoop.security.authorization", "true");
            conf.set("hadoop.security.token.renewer.master.interval", "60*60*23");
            conf.set("hadoop.security.token.renewer.master.timeout", "300");*/
            KerberosAuthUtils.initKerberosAuth(Constants.KRB5_CONF_PATH, Constants.SYS_UCV_TEST_FOR_MAPREDUCE_PRINCIPAL, Constants.SYS_UCV_TEST_FOR_MAPREDUCE_KEYTAB_PATH);

//            UserGroupInformation.setConfiguration(conf);
//            UserGroupInformation.loginUserFromKeytab("hdfs-6cd43e43-1ea1-45f7-afad-fdb9a888cf27@BCHKDC", "src/main/resources/hdfs.headless.keytab");
//            UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
//            System.out.println(currentUser.toString());
            FileSystem fs = FileSystem.get(conf);

            // 示例：列出根目录下的文件和文件夹
            FileStatus[] fileStatuses = fs.listStatus(new Path("hdfs://cmss/data/test/checkpoint"));
            for (FileStatus fileStatus :
                    fileStatuses) {
                System.out.println(fileStatus.getPath().getName());
            }
            fs.close();
            System.out.println("Operation successful.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
