package com.travelsky.dataplatform.utils;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimSummaryModel;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author kuangaihua
 * @date 2025/7/28 13:42
 */
public class GetTableSqlTest {
    public static void main(String[] args) throws SQLException {
        UserDimModel model = new UserDimModel();
        model.setPkId("123");
        model.setCrmCustomerId("123");
        model.setLyCardNumber("123456");
        System.out.println(
                GetTableSql.getFieldStr(model.getPkId()) +
                        GetTableSql.getFieldStr(model.getCrmCustomerId()) +
                        GetTableSql.getFieldStrEnd(model.getLyCardNumber())
        );

    }
}
