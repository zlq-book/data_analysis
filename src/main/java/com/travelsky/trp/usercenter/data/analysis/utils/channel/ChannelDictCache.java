package com.travelsky.trp.usercenter.data.analysis.utils.channel;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.DorisUtils;
import com.travelsky.trp.usercenter.data.analysis.utils.ChannelInfo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class ChannelDictCache {

    private final Map<String, ChannelInfo> byLevel4Name = new HashMap<>();
    private final Map<String, ChannelInfo> byLevel4Code = new HashMap<>();
    private final Map<String, ChannelInfo> byLevel3Name = new HashMap<>();
    private final Map<String, ChannelInfo> byLevel3Code = new HashMap<>();
    private final Map<String, ChannelInfo> byLevel2Name = new HashMap<>();
    private final Map<String, ChannelInfo> byLevel2Code = new HashMap<>();
    private final Map<String, ChannelInfo> byLevel1Name = new HashMap<>();
    private final Map<String, ChannelInfo> byLevel1Code = new HashMap<>();
    private final Map<Long, ChannelInfo> byId = new HashMap<>();

    public Map<String, ChannelInfo> getByLevel1Name() {
        return byLevel1Name;
    }

    public Map<String, ChannelInfo> getByLevel1Code() {
        return byLevel1Code;
    }

    public Map<String, ChannelInfo> getByLevel2Name() {
        return byLevel2Name;
    }

    public Map<String, ChannelInfo> getByLevel2Code() {
        return byLevel2Code;
    }

    public Map<String, ChannelInfo> getByLevel3Name() {
        return byLevel3Name;
    }

    public Map<String, ChannelInfo> getByLevel3Code() {
        return byLevel3Code;
    }

    public Map<String, ChannelInfo> getByLevel4Name() {
        return byLevel4Name;
    }

    public Map<String, ChannelInfo> getByLevel4Code() {
        return byLevel4Code;
    }

    public Map<Long, ChannelInfo> getById() {
        return byId;
    }

    public void load() throws Exception {
        Connection conn = null;
        Statement stmt = null;
        ResultSet res = null;
        try {
            conn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DIM_USER, Constants.DIM_PWD);
            stmt = DorisUtils.getStatement(conn);
            String sql = new StringBuilder().append("SELECT ID, LEVEL1_CODE, LEVEL1_NAME, LEVEL2_CODE, LEVEL2_NAME, ")
                    .append("LEVEL3_CODE, LEVEL3_NAME, LEVEL4_CODE, LEVEL4_NAME, FFP_DEPT_FLAG ")
                    .append("FROM ")
                    .append(Constants.DIM_DB)
                    .append(".T_DIM_FFP_CHANNEL_DICT").toString();
            res = DorisUtils.getDorisResult(stmt, sql);
            while (res != null && res.next()) {
                String l1c = trim(res.getString("LEVEL1_CODE"));
                String l1n = trim(res.getString("LEVEL1_NAME"));
                String l2c = trim(res.getString("LEVEL2_CODE"));
                String l2n = trim(res.getString("LEVEL2_NAME"));
                String l3c = trim(res.getString("LEVEL3_CODE"));
                String l3n = trim(res.getString("LEVEL3_NAME"));
                String l4c = trim(res.getString("LEVEL4_CODE"));
                String l4n = trim(res.getString("LEVEL4_NAME"));
                String ffpDeptFlag = trim(res.getString("FFP_DEPT_FLAG"));

                ChannelInfo info = new ChannelInfo(l1c, l1n, l2c, l2n, l3c, l3n, l4c, l4n, ffpDeptFlag);
                long id = 0L;
                try {
                    id = res.getLong("ID");
                } catch (Exception ignored) {
                }
                byId.put(id, info);

                putPreferDeeper(byLevel1Name, l1n, info);
                putPreferDeeper(byLevel1Code, l1c, info);
                putPreferDeeper(byLevel2Name, l2n, info);
                putPreferDeeper(byLevel2Code, l2c, info);
                putPreferDeeper(byLevel3Name, l3n, info);
                putPreferDeeper(byLevel3Code, l3c, info);
                putPreferDeeperWithFfpDeptFlag(byLevel4Name, l4n, info);
                putPreferDeeperWithFfpDeptFlag(byLevel4Code, l4c, info);
            }
        } finally {
            DorisUtils.close(conn, stmt, res);
        }
    }

    public ChannelInfo matchByOperateDept(String operateDept) {
        return matchByOperateDept(operateDept, null);
    }

    public ChannelInfo matchByOperateDept(String operateDept, String ffpDeptFlag) {
        if (operateDept == null) return null;

        if (StringNotBlank(ffpDeptFlag)) {
            String compositeKey = operateDept + "_" + ffpDeptFlag;
            ChannelInfo info = byLevel4Name.get(compositeKey);
            if (info != null) return info;
            info = byLevel4Code.get(compositeKey);
            if (info != null) return info;
        }

        ChannelInfo info = byLevel4Name.get(operateDept);
        if (info != null) return info;
        info = byLevel4Code.get(operateDept);
        if (info != null) return info;
        info = byLevel3Name.get(operateDept);
        if (info != null) return info;
        info = byLevel3Code.get(operateDept);
        if (info != null) return info;
        info = byLevel2Name.get(operateDept);
        if (info != null) return info;
        info = byLevel2Code.get(operateDept);
        if (info != null) return info;
        info = byLevel1Name.get(operateDept);
        if (info != null) return info;
        return byLevel1Code.get(operateDept);
    }

    private static String trim(String s) {
        return s == null ? null : s.trim();
    }

    private static int depth(ChannelInfo info) {
        if (StringNotBlank(info.getLevel4Code()) || StringNotBlank(info.getLevel4Name())) return 4;
        if (StringNotBlank(info.getLevel3Code()) || StringNotBlank(info.getLevel3Name())) return 3;
        if (StringNotBlank(info.getLevel2Code()) || StringNotBlank(info.getLevel2Name())) return 2;
        if (StringNotBlank(info.getLevel1Code()) || StringNotBlank(info.getLevel1Name())) return 1;
        return 0;
    }

    private static boolean StringNotBlank(String s) {
        return s != null && !s.trim().isEmpty();
    }

    private static void putPreferDeeper(Map<String, ChannelInfo> map, String key, ChannelInfo value) {
        if (key == null || key.isEmpty()) return;
        ChannelInfo exist = map.get(key);
        if (exist == null) {
            map.put(key, value);
            return;
        }
        if (depth(value) > depth(exist)) {
            map.put(key, value);
        }
    }

    private static void putPreferDeeperWithFfpDeptFlag(Map<String, ChannelInfo> map, String key, ChannelInfo value) {
        if (key == null || key.isEmpty()) return;

        String ffpDeptFlag = value.getFfpDeptFlag();
        boolean hasFfpDeptFlag = StringNotBlank(ffpDeptFlag);

        if (hasFfpDeptFlag) {
            String compositeKey = key + "_" + ffpDeptFlag;
            ChannelInfo exist = map.get(compositeKey);
            if (exist == null) {
                map.put(compositeKey, value);
            } else if (depth(value) > depth(exist)) {
                map.put(compositeKey, value);
            }

            ChannelInfo existByKey = map.get(key);
            if (existByKey == null) {
                map.put(key, value);
            } else {
                String existFfpDeptFlag = existByKey.getFfpDeptFlag();
                if (!StringNotBlank(existFfpDeptFlag)) {
                    if (depth(value) >= depth(existByKey)) {
                        map.put(key, value);
                    }
                }
            }
        } else {
            ChannelInfo exist = map.get(key);
            if (exist == null) {
                map.put(key, value);
            } else {
                String existFfpDeptFlag = exist.getFfpDeptFlag();
                if (!StringNotBlank(existFfpDeptFlag)) {
                    if (depth(value) > depth(exist)) {
                        map.put(key, value);
                    }
                }
            }
        }
    }
}


