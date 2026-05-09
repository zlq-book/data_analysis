package com.travelsky.dataplatform.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.util.Collection;
import java.util.Timer;
import java.util.TimerTask;

/**
 * @author kuangaihua
 * @date 2025/8/15 21:02
 */
public class KerberosAuthUtils {
    private static final long REFRESH_INTERVAL = 3600000 * 23; // 23小时刷新一次
    private static Timer refreshTimer;

    public static UserGroupInformation initKerberosAuth(String krb5ConfPath,
                                        String principal,
                                        String keytabPath) {
        try {
            // 1. 首次登录
            UserGroupInformation userGroupInformation = loginFromKeytab(principal, keytabPath,krb5ConfPath);
            // 2. 启动定时刷新任务
            startRefreshTask(principal, keytabPath,krb5ConfPath);
            return userGroupInformation;
        } catch (Exception e) {
            throw new RuntimeException("Kerberos初始化失败", e);
        }
    }

    private static UserGroupInformation loginFromKeytab(String principal, String keytabPath,String krb5ConfPath) throws Exception {
        // 1. 初始化Kerberos配置
        System.setProperty("java.security.krb5.conf", krb5ConfPath);
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://cmss");
        conf.set("dfs.nameservices", "cmss");
        conf.set("dfs.ha.namenodes.cmss", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.cmss.nn1", "emr-master-001.novalocal:8020");
        conf.set("dfs.namenode.rpc-address.cmss.nn2", "emr-master-ha-001.novalocal:8020");
        conf.set("dfs.client.failover.proxy.provider.cmss",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hadoop.security.authorization", "true");
        UserGroupInformation.setConfiguration(conf);
//        UserGroupInformation userGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabPath);
        UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
        UserGroupInformation userGroupInformation = UserGroupInformation.getLoginUser();
        return userGroupInformation;
    }

    private static void startRefreshTask(String principal, String keytabPath,String krb5ConfPath) {
        refreshTimer = new Timer("Kerberos-Refresh-Timer", true);
        refreshTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    if (UserGroupInformation.isLoginKeytabBased()) {
                        UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
                    }
                } catch (Exception e) {
                    System.err.println("Kerberos刷新失败，尝试重新登录");
                    try {
                        loginFromKeytab(principal, keytabPath,krb5ConfPath);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }, REFRESH_INTERVAL, REFRESH_INTERVAL);
    }

    public static void shutdown() {
        if (refreshTimer != null) {
            refreshTimer.cancel();
        }
    }
}
