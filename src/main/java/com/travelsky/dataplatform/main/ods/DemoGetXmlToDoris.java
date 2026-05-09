package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.TOdsHytdTManageOthercard;
import com.travelsky.dataplatform.source.WholeFileSource;
import com.travelsky.dataplatform.utils.DocumentUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.KerberosAuthUtils;
import com.travelsky.dataplatform.utils.XpathUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.w3c.dom.Document;

import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @author kuangaihua
 * @date 2025/7/14 9:47
 */
public class DemoGetXmlToDoris {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        KerberosAuthUtils.initKerberosAuth(Constants.KRB5_CONF_PATH, Constants.SYS_UCV_TEST_FOR_MAPREDUCE_PRINCIPAL, Constants.SYS_UCV_TEST_FOR_MAPREDUCE_KEYTAB_PATH);

        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        WholeFileSource wholeFileSource = new WholeFileSource("E:\\航信资料\\日常工作\\璇玑数据平台\\需求\\trp智能化\\产品推荐\\高频数据\\开发用样例\\Book.xml");
        DataStreamSource<String> fileDataStreamSource = env.addSource(wholeFileSource);
        SingleOutputStreamOperator<TOdsHytdTManageOthercard> tOdsHytdTManageOthercardModelSource = fileDataStreamSource.map(new MapFunction<String, TOdsHytdTManageOthercard>() {
            @Override
            public TOdsHytdTManageOthercard map(String s) throws Exception {
                TOdsHytdTManageOthercard tOdsHytdTManageOthercard = new TOdsHytdTManageOthercard();
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                tOdsHytdTManageOthercard.setETL_DATE(Date.valueOf(LocalDate.parse(etlDate, formatter)));
                Document document;
                try {
                    document = DocumentUtils.string2Document(s);
                } catch (Exception e) {
                    return null;
                }
//                获取event
                String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
//                获取pnr
                String PNR = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@RecordLocator");
                System.out.println(event);
                System.out.println(PNR);
                tOdsHytdTManageOthercard.setID("1002");
                return tOdsHytdTManageOthercard;
            }
        });
        // 输出结果查看
        tOdsHytdTManageOthercardModelSource.print();
/*        //创建dorisSink
        DorisSink<TOdsHytdTManageOthercard> dorisSink = FlinkDorisUtils.creatDorisSink(Constants.ODS_DB, "T_ODS_HYTD_T_MANAGE_OTHERCARD", Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT, Constants.ODS_USER, Constants.ODS_PWD);
        //数据写入doris
        tOdsHytdTManageOthercardModelSource.sinkTo(dorisSink);*/
        env.execute();

    }
}
