package com.travelsky.trp.usercenter.data.analysis.transform.labels;

import com.travelsky.dataplatform.constans.CreateTableSql;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;


public class BookingSegDataStream {
    static final Logger logger = LoggerFactory.getLogger(BookingSegDataStream.class);

    public static void result(StreamTableEnvironment tEnv, String etlDate) {
        // 创建Doris目标表
        tEnv.executeSql(CreateTableSql.DWD_BOOKING_SEG_FACT);

        LocalDate localDate = LocalDate.parse(etlDate);

        // 从 Doris 表中读取数据
        Table dorisTable = tEnv.sqlQuery( "SELECT \n" +
                        "PK_ID ,\n" +
                        "PNR_NUMBER ,\n" +
                        "FK_BOOKING_DATE ,\n" +
                        "FK_DEPAIRPORT ,\n" +
                        "FK_SEG_DATE ,\n" +
                        "PEER_SENIOR ,\n" +
                        "AK_PEER_CHD ,\n" +
                        "FK_PASSENGER_USER_TID ,\n" +
                        "FK_BOOKING_USER_TID  \n" +
                        "FROM T_DWD_BOOKING_SEG_FACT  "
                        + " WHERE  SYSTEM_CREATETIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '"
                        + etlDate + " 23:59:59.999' "
                        + " AND DATA_ACTIVE =true "
                );

        DataStream<Row> rowStream = tEnv.toDataStream(dorisTable);


        // 是否与老人同行 是否与儿童同行 是否为自己订票
        BsDataStreamHandle.output(rowStream);


    }
}
