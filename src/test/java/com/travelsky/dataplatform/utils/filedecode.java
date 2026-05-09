package com.travelsky.dataplatform.utils;

import java.io.*;

import com.travelsky.dataplatform.constans.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


import javax.servlet.http.HttpServletResponse;
import java.net.URI;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class filedecode {
    public static void main(String[] args) throws UnsupportedEncodingException {
String strongid="1";
        String sql = "SELECT TT1.* FROM (SELECT\n" +
                "    TID,\n" +
                "    map['FFP'] as FFP,\n" +
                "    map['LYC'] as LYC,\n" +
                "    map['PHONE'] as PHONE,\n" +
                "    map['CCID'] as CCID,\n" +
                "    map['CD'] as CD,\n" +
                "    map['CS'] as CS,\n" +
                "    map['CT'] as CT,\n" +
                "    map['CW'] as CW,\n" +
                "    map['DP'] as DP,\n" +
                "    map['EP'] as EP,\n" +
                "    map['FR'] as FR,\n" +
                "    map['HP'] as HP,\n" +
                "    map['HR'] as HR,\n" +
                "    map['IS'] as `IS`,\n" +
                "    map['MP'] as MP,\n" +
                "    map['NC'] as NC,\n" +
                "    map['NI'] as NI,\n" +
                "    map['OF'] as OF,\n" +
                "    map['OP'] as OP,\n" +
                "    map['OT'] as OT,\n" +
                "    map['PE'] as PE,\n" +
                "    map['PI'] as PI,\n" +
                "    map['PO'] as PO,\n" +
                "    map['PP'] as PP,\n" +
                "    map['PR'] as PR,\n" +
                "    map['RP'] as RP,\n" +
                "    map['RR'] as RR,\n" +
                "    map['RT'] as RT,\n" +
                "    map['SC'] as SC,\n" +
                "    map['SE'] as SE,\n" +
                "    map['SI'] as SI,\n" +
                "    map['SM'] as SM,\n" +
                "    map['ST'] as ST,\n" +
                "    map['TC'] as TC,\n" +
                "    map['TP'] as TP,\n" +
                "    map['VC'] as VC,\n" +
                "    map['WP'] as WP,\n" +
                "    map['WS'] as WS\n" +
                "FROM\n" +
                "    (\n" +
                "    SELECT\n" +
                "        TID,\n" +
                "        map_agg(STRONGID_TYPE, STRONGID) as map\n" +
                "    FROM\n" +
                "        (\n" +
                "        SELECT\n" +
                "            TID,\n" +
                "            STRONGID_TYPE,\n" +
                "            GROUP_CONCAT(STRONGID , ',') AS STRONGID\n" +
                "        from\n" +
                "            " + Constants.DWQ_DB + ".T_DIM_STRONGID\n" +
                "        WHERE\n" +
                "            TID IN(\n" +
                "SELECT TID FROM " + Constants.DWQ_DB + ".T_DIM_STRONGID \n" +
                "WHERE STRONGID IN(SELECT STRONGID FROM " + Constants.DWQ_DB + ".T_DIM_STRONGID WHERE STRONGID = '" + strongid + "' AND STRONGID_STATUS = 1 GROUP BY STRONGID HAVING COUNT(1)>1 ) \n" +
                "    )\n" +
                "            AND STRONGID_STATUS = 1\n" +
                "        GROUP BY\n" +
                "            TID,\n" +
                "            STRONGID_TYPE\n" +
                "    ) T\n" +
                "    GROUP BY\n" +
                "        TID) T1) TT1 ";
/*        String encodedUrl = "0gBjEoLsHvSsKf8eDyK2dOnzG9o%3D";
        String decodedUrl = java.net.URLDecoder.decode(encodedUrl, "UTF-8");*/
        System.out.println(sql);
//        test();
    }

    public static void test() {
        //        FileSystem fs = null;
        FSDataInputStream fis = null;
        FileSystem plaintextFs = null;
        BufferedInputStream bis = null;
        byte[] buffer = new byte[1024];
        try {
//            fs = FileSystem.get(new URI(hdfsProperty.getAddress()), conf, hdfsProperty.getUser());
            String filePath = "hdfs://cmss/data/test/encrypt/user_grouping/a.txt";
            //String filePath = hdfsProperty.getUserGroupingCsvPath() + "a.txt";
            plaintextFs = FileSystem.get(buildHdfsConfiguration());
            Path path = new Path(filePath);

//            Path path = new Path(filePath);

            if (!plaintextFs.exists(path)) {

            }
            FileStatus[] fileStatuses =plaintextFs.listStatus(path);
            for (FileStatus fileStatus :
                    fileStatuses) {
                System.out.println(fileStatus.getPath().getName());
            }
            fis = plaintextFs.open(path);
            bis = new BufferedInputStream(fis);
            OutputStream outputStream = new FileOutputStream("C:\\Users\\DELL\\Desktop\\output.txt");

            int i = bis.read(buffer);
            while (i != -1) {
                outputStream.write(buffer,0, i);
                i = bis.read(buffer);
            }
        } catch (Exception e) {

        } finally {
            if (bis != null) {
                try {
                    bis.close();
                } catch (IOException e) {

                }
            }
            IOUtils.closeStream(fis);
            IOUtils.closeStream(plaintextFs);
        }

    }
    private static Configuration buildHdfsConfiguration() {
        Configuration conf = new Configuration();
        try {
            conf.set("fs.defaultFS", "hdfs://cmss");
            conf.set("dfs.nameservices", "cmss");
            conf.set("dfs.ha.namenodes.cmss", "nn1,nn2");
            conf.set("dfs.namenode.rpc-address.cmss.nn1", "emr-master-001.novalocal:8020");
            conf.set("dfs.namenode.rpc-address.cmss.nn2", "emr-master-ha-001.novalocal:8020");
            conf.set("dfs.client.failover.proxy.provider.cmss",
                    "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
            KerberosAuthUtils.initKerberosAuth("src/main/resources/krb5.conf",
                    "sys_ucv_test_for_mapreduce@BCHKDC", "src/main/resources/sys_ucv_test_for_mapreduce.keytab");


        } catch (Exception e) {
            e.printStackTrace();
        }
        return conf;
    }

}
