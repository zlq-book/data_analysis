package com.travelsky.trp.usercenter.data.analysis.transform.labels;

import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.DataSource;
import com.travelsky.dataplatform.utils.FieldType;
import com.travelsky.dataplatform.utils.IdMapping;
import com.travelsky.dataplatform.utils.NormalizationUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.transform.lyx.LmMlsLluToDimCertDimTrans;
import com.travelsky.trp.usercenter.data.analysis.transform.lyx.LmMlsLluToDimLyxDimTrans;
import com.travelsky.trp.usercenter.data.analysis.transform.lyx.LmMlsLluToDimMobileDimTrans;
import com.travelsky.trp.usercenter.data.analysis.transform.lyx.LmMlsLluToDimUserDimTrans;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;


public class TickingTicDataStream {
    static final Logger logger = LoggerFactory.getLogger(TickingTicDataStream.class);

    public static void result(StreamTableEnvironment tEnv, String etlDate) {
        // 创建Doris目标表
        tEnv.executeSql(CreateTableSql.DWD_TICKING_TIC_FACT);

        LocalDate localDate = LocalDate.parse(etlDate);

        // 从 Doris 表中读取数据
        Table dorisTable = tEnv.sqlQuery( "SELECT " +
                        "PK_ID ," +
                        "TICKET_NUMBER ," +
                        "AK_ADVBOOK_DAY \n" +
                        "FROM T_DWD_TICKING_TIC_FACT   "
                        + " WHERE  SYSTEM_CREATETIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '"
                        + etlDate + " 23:59:59.999' "
                        + " AND DATA_ACTIVE =true "
                );

        DataStream<Row> rowStream = tEnv.toDataStream(dorisTable);

        // 提前出票天数（客票级）
        TtDataStreamQueryTickingSeg.output(rowStream);


    }
}
