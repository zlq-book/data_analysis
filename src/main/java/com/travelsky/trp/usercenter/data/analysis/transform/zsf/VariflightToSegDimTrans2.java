package com.travelsky.trp.usercenter.data.analysis.transform.zsf;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.SegDimModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class VariflightToSegDimTrans2 {

    private static final Logger logger = LoggerFactory.getLogger(VariflightToSegDimTrans2.class);

    // 将 Object 转为 String，支持 null
    private static String toStringSafe(Object obj) {
        return obj != null ? obj.toString() : null;
    }

    // 存储 UP_LOCATION_DIS_LOCATION -> DIST 的映射（static 确保全局唯一，或在 open 中初始化）
    public static Map<String, String> upDisToDistMap;

    public static void result(StreamTableEnvironment tEnv, String etlDate) throws Exception {

        // 注册表
        tEnv.executeSql(CreateTableSql.DIM_SEG_DIM);
        tEnv.executeSql(CreateTableSql.SC_TB_TICKET_PRICE_ORDER);

        // 开始时间
        long startTime = System.currentTimeMillis();

        // 查询 Doris 表
        String query = "SELECT \n" +
                "    SEGMENT_KEY,\n" +
                "    AIRLINE,\n" +
                "    OPERAT_AIRLINE,\n" +
                "    OPERAT_FLIGHT_NUM,\n" +
                "    DEPARTURE_DATE,\n" +
                "    DEPARTURE_TIME,\n" +
                "    ARRIVAL_DATE,\n" +
                "    ARRIVAL_TIME,\n" +
                "    DURATION,\n" +
                "    AIRTYPE,\n" +
                "    MARKET_AIRLINE,\n" +
                "    MARKET_FLIGHT_NUM,\n" +
                "    DISTANCE_TPM,\n" +
                "    IS_DELAYED,\n" +
                "    IS_CANCELLED,\n" +
                "    IS_CODE_SHARE,\n" +
                "    CREATE_TIME,\n" +
                "    UPDATE_TIME\n" +
                "FROM T_DIM_SEG_DIM \n" +
                "LIMIT 100 ";

        Table dorisTable = tEnv.sqlQuery(query);
        DataStream<Row> rowDataStream = tEnv.toChangelogStream(dorisTable);
        rowDataStream.print("Doris Table Data: ");


        String TICKET_PRICE_ORDER = "SELECT \n" +
                "    UP_LOCATION,\n" +
                "    DIS_LOCATION,\n" +
                "    DIST,\n" +
                "    EX_DATE\n" +
                "FROM (\n" +
                "    SELECT \n" +
                "        UP_LOCATION,\n" +
                "        DIS_LOCATION,\n" +
                "        DIST,\n" +
                "        EX_DATE,\n" +
                "        ROW_NUMBER() OVER (PARTITION BY UP_LOCATION, DIS_LOCATION ORDER BY EX_DATE DESC) AS rn\n" +
                "    FROM SC_TB_TICKET_PRICE_ORDER\n" +
                "    WHERE DIST IS NOT NULL\n" +
                ") t\n" +
                "WHERE rn = 1";
        Table titket_price = tEnv.sqlQuery(TICKET_PRICE_ORDER);
        DataStream<Row> changelogStream  = tEnv.toChangelogStream(titket_price);

        // -------------------------- 核心：构建 UP_LOCATION_DIS_LOCATION -> DIST 的 Map --------------------------
        CloseableIterator<Row> ticketPriceIterator = changelogStream.executeAndCollect();
        List<Row> ticketPriceList = new ArrayList<>();
        try {
            // 手动遍历迭代器收集数据
            while (ticketPriceIterator.hasNext()) {
                ticketPriceList.add(ticketPriceIterator.next());
            }
        } catch (Exception e) {
            logger.error("收集机场对数据失败", e);
            throw e; // 抛出异常，中断任务（根据业务可调整）
        } finally {
            // 强制关闭迭代器，释放资源
            if (ticketPriceIterator != null) {
                ticketPriceIterator.close();
            }
        }
        // 构建 Map
        upDisToDistMap = new HashMap<>(ticketPriceList.size());
        for (Row row : ticketPriceList) {
            String upLocation = toStringSafe(row.getField("UP_LOCATION"));
            String disLocation = toStringSafe(row.getField("DIS_LOCATION"));
            String dist = toStringSafe(row.getField("DIST"));
            String key = upLocation + "_" + disLocation;
            upDisToDistMap.put(key, dist);
        }
        logger.info("成功构建机场对-DIST映射，共 {} 条数据", upDisToDistMap.size());

        SingleOutputStreamOperator<SegDimModel> asyncResultStream = AsyncDataStream.orderedWait(
                rowDataStream.filter(row -> row != null && toStringSafe(row.getField("AIRLINE")) != null),
                new AsyncFlightFetchFunction(),
                30_000,   // 超时时间30秒
                TimeUnit.MILLISECONDS,
                3        // 最大并发度，调给接口压力和吞吐留空间
        );

        // 写入 Doris
        logger.info("写入 Doris 表: DB={}, Table={}", Constants.DIM_DB, "T_DIM_SEG_DIM");
        DorisSink<SegDimModel> dorisSink1 = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_SEG_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD
        );
        asyncResultStream.sinkTo(dorisSink1);
        asyncResultStream.print("T_DIM_SEG_DIM");
        // 结束时间
        long endTime = System.currentTimeMillis();
        logger.info("写入 Doris 表: DB={}, Table={}, 耗时={}", Constants.DIM_DB, "T_DIM_SEG_DIM", (endTime - startTime) / 1000.0);
    }

    public static Map<String, String> getUpDisToDistMap() {
        return upDisToDistMap;
    }
}