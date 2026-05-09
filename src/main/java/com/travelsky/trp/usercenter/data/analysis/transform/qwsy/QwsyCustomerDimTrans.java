package com.travelsky.trp.usercenter.data.analysis.transform.qwsy;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.DateTimeUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.IdMapping;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.QwsyDimModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * 企微小山客户信息维表数据转换
 * 从 ODS 层的企微客户信息表和标签表关联，转换到 DIM 层的企微小山客户信息维表
 */
public class QwsyCustomerDimTrans {

    static final Logger logger = LoggerFactory.getLogger(QwsyCustomerDimTrans.class);

    // 将 Object 转为 String，支持 null
    private static String toStringSafe(Object obj) {
        return obj != null ? obj.toString() : null;
    }

    // 将 dateTime 转为 LocalDateTime 字符串
    private static String formatDateTimeSafe(Object ts) {
        if (ts != null) {
            LocalDateTime s = (LocalDateTime) ts;
            return s.toString().replace("T", " ");
        }
        return null;
    }

    public static void result(StreamTableEnvironment tEnv, String etlDate) throws Exception {

        // 注册源表
        tEnv.executeSql(CreateTableSql.T_ODS_QWSY_CUSTOMER_INFO);
        tEnv.executeSql(CreateTableSql.T_ODS_QWSY_CUSTOMER_TAG_INFO);

        // 查询 - 关联客户信息表和标签表，聚合标签
        String query = "SELECT \n" +
                "    a.ID AS ID,\n" +
                "    a.CORP_ID AS CORP_ID,\n" +
                "    a.EXTERNAL_USER_ID AS EXTERNAL_USER_ID,\n" +
                "    a.EXTERNAL_USER_NAME AS EXTERNAL_USER_NAME,\n" +
                "    a.EXTERNAL_TYPE AS EXTERNAL_TYPE,\n" +
                "    a.FOLLOW_USER_ID AS FOLLOW_USER_ID,\n" +
                "    a.FOLLOW_USER_NAME AS FOLLOW_USER_NAME,\n" +
                "    a.FOLLOW_REMARK_MOBILES AS FOLLOW_REMARK_MOBILES,\n" +
                "    a.FOLLOW_DESCRIPTION AS FOLLOW_DESCRIPTION,\n" +
                "    a.FOLLOW_ADD_TIME AS FOLLOW_ADD_TIME,\n" +
                "    a.FOLLOW_TAGS AS FOLLOW_TAGS,\n" +
                "    a.IS_DEL AS IS_DEL,\n" +
                "    a.IS_LOSS AS IS_LOSS,\n" +
                "    a.CREATE_TIME AS CREATE_TIME,\n" +
                "    a.UPDATE_TIME AS UPDATE_TIME,\n" +
                "    c.GROUP_NAME AS GROUP_NAME,\n" +
                "    c.TAG_NAME AS TAG_NAMES  -- 使用子查询中合并后的标签\n" +
                "FROM T_ODS_QWSY_CUSTOMER_INFO a\n" +
                "LEFT JOIN (\n" +
                "    SELECT \n" +
                "        EXTERNAL_USER_ID,\n" +
                "        GROUP_NAME, \n" +
                "        LISTAGG(b.TAG_NAME, ',') AS TAG_NAME  -- 合并同一用户的所有标签\n" +
                "    FROM T_ODS_QWSY_CUSTOMER_TAG_INFO b \n" +
                "    GROUP BY GROUP_NAME,EXTERNAL_USER_ID  -- 按用户分组，确保每个用户一条记录\n" +
                ") c ON a.EXTERNAL_USER_ID = c.EXTERNAL_USER_ID";

        Table dorisTable = tEnv.sqlQuery(query);



        DataStream<Row> rowDataStream = tEnv.toChangelogStream(dorisTable)
                .filter(row -> row.getKind().equals(RowKind.INSERT));;

        // 映射为 QwsyDimModel - 企微小山客户信息维表
        SingleOutputStreamOperator<QwsyDimModel> qwsyDimStream = rowDataStream.map(row -> {
            QwsyDimModel model = new QwsyDimModel();

            // 获取核心字段
            String corpId = toStringSafe(row.getField("CORP_ID"));
            String externalUserId = toStringSafe(row.getField("EXTERNAL_USER_ID"));
            String externalUserName = toStringSafe(row.getField("EXTERNAL_USER_NAME"));

            // 1、生成 T_ID, 使用IDMapping
            Tid tid = new Tid();
            tid.setTid(externalUserId);
            tid.setCrmCustomerId(externalUserId);
            String tidstr = IdMapping.idMappingFunction(tid, "QWSY");
            model.settId(tidstr);

            // 2、生成主键 SYSTEM_KEY
            String systemKey = (tidstr != null ? tidstr : "") +
                             (externalUserId != null ? externalUserId : "");
            model.setSystemKey(systemKey);

            // 3、企业ID
            model.setCompanyId(corpId);

            // 4、企微客户ID
            model.setQwCustomerId(externalUserId);

            // 5、企微客户等级
            // 根据 EXTERNAL_TYPE 判断客户等级
            String externalTypeObj = toStringSafe(row.getField("GROUP_NAME"));
            model.setQwCustomerLevel(externalTypeObj);

            // 6、企微客户个人标签
            // 合并 FOLLOW_TAGS 和聚合的 TAG_NAMES

            String tagNames = toStringSafe(row.getField("TAG_NAMES"));
            model.setQwCustomerTags(tagNames);

            // 写入或更新的系统时间，年月日时分秒
            String currentDateTime = DateTimeUtils.getCurrentDateTime();
            model.setUpdateTime(currentDateTime);

            // 第一次写入的系统时间，年月日时分秒
            model.setCreateTime(currentDateTime);

            return model;
        });

        // 打印调试信息
        qwsyDimStream.print("qwsyCustomerDimTrans_stream");

        // 写入 Doris
        logger.info("连接 Doris 参数: DB={}, Table={}, FE={}, BE={}, User={}",
                Constants.DIM_DB, "T_DIM_QWSY_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER);

        DorisSink<QwsyDimModel> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_QWSY_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD);

        // 数据写入 Doris
        qwsyDimStream.sinkTo(dorisSink);

        logger.info("企微小山客户信息维表数据转换完成");
    }
}