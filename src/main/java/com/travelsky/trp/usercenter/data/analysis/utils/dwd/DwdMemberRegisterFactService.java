package com.travelsky.trp.usercenter.data.analysis.utils.dwd;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.DorisUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class DwdMemberRegisterFactService {

    private static volatile Connection conn;
    private static volatile Statement stmt;

    private static void ensureOpen() {
        if (stmt == null) {
            synchronized (DwdMemberRegisterFactService.class) {
                if (stmt == null) {
                    Connection c = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
                    Statement s = DorisUtils.getStatement(c);
                    conn = c;
                    stmt = s;
                }
            }
        }
    }

    public static boolean shouldSetClkChannel(String pkId, String registerDate) {
        if (pkId == null || registerDate == null || registerDate.length() < 10) {
            return false;
        }
        String date = registerDate.substring(0, 10);
        if (date.compareTo("2023-12-01") < 0) {
            return false;
        }
        if (date.compareTo("2025-09-03") >= 0) {
            return false;
        }
        ensureOpen();
        if (stmt == null) {
            return false;
        }
        String sql = "SELECT FROM_CRM_MEMBER, FROM_INTEGRATION " +
                "FROM " + Constants.DWD_DB + ".T_DWD_MEMBER_REGISTER_FACT " +
                "WHERE PK_ID = '" + pkId.replace("'", "''") + "' LIMIT 1";
        ResultSet rs = DorisUtils.getDorisResult(stmt, sql);
        try {
            if (rs == null || !rs.next()) {
                return true;
            }
            int crmVal = rs.getInt("FROM_CRM_MEMBER");
            boolean isFromCrm = !rs.wasNull() && crmVal == 1;
            int integVal = rs.getInt("FROM_INTEGRATION");
            boolean isFromIntegration = !rs.wasNull() && integVal == 1;
            return !isFromCrm && !isFromIntegration;
        } catch (Exception e) {
            return false;
        }
    }
}


