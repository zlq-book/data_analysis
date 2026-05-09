package com.travelsky.trp.usercenter.data.analysis.transform.lyx;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.DataSource;
import com.travelsky.dataplatform.utils.FieldType;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.NormalizationUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.AuisSegFactModel;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public class OtiOfiOpiToAuisSegFactTrans {

    static final Logger logger = LoggerFactory.getLogger(OtiOfiOpiToAuisSegFactTrans.class);

    public static void output(DataStream<Row> rowDataStream) {

        SingleOutputStreamOperator<AuisSegFactModel> modelStream = rowDataStream.filter(row -> {
            boolean flag = row.getField("INSURE_STATUS") == null || row.getField("OPT_STATUS") == null
                    || row.getField("TICKET_NUM") == null
                    || row.getField("INSURE_SERIAL_NUMBER") == null;
            if (flag) {
                logger.info("入库保险事实表-航段级 事实表异常，oriTable：T_ODS_LYX_ORDER_TICKET_INFO，" + row.toString());
            }
            return !flag;
        }).flatMap((Row row, Collector<AuisSegFactModel> out) -> {
            if (!"0".equals(row.getField("INSURE_STATUS").toString())
                    && "1".equals(row.getField("OPT_STATUS").toString())
                    && null != row.getField("TICKET_NUM")
                    && null != row.getField("INSURE_SERIAL_NUMBER")) {
                AuisSegFactModel model = new AuisSegFactModel();

                // 出票时间

                model.setPkId(row.getField("TICKET_NUM").toString().replace("-", "")
                        + row.getField("INSURE_SERIAL_NUMBER").toString());
                model.setAkOrdernum(row.getField("ORDER_NO") == null ? null :row.getField("ORDER_NO").toString());
                model.setAkTiknum(row.getField("TICKET_NUM").toString());
                model.setFkBkauisDate(row.getField("CREATE_TIME") == null ? null :row.getField("CREATE_TIME").toString().substring(0, 10));
                model.setFkBkauisTime(row.getField("CREATE_TIME") == null ? null :row.getField("CREATE_TIME").toString().substring(11));
                // 旅客姓名
                String psgName = row.getField("PSG_NAME_CN") == null ?
                        (row.getField("PSG_NAME_EN") == null ? "" : row.getField("PSG_NAME_EN").toString())
                        : row.getField("PSG_NAME_CN").toString();
                model.setCnName(psgName);
                model.setAkCertType(row.getField("CERT_TYPE") == null ? null : row.getField("CERT_TYPE").toString());
                model.setCertNumber(row.getField("CERT_NUM") == null ? null : row.getField("CERT_NUM").toString());
                // 乘机人TID
                if (null != row.getField("CERT_TYPE") && null != row.getField("CERT_NUM")) {
                    model.setFkPassengerUserTid(row.getField("PSG_TID").toString());
                }

                // TID
                model.setFkBookingUserTid(row.getField("TID").toString());

                model.setFkBookingUserOriginId(row.getField("LY_CARD").toString());
                model.setFkSegTime(row.getField("DEPATURE_TIME") == null ? null : row.getField("DEPATURE_TIME").toString().substring(11));
                model.setFkSegDate(row.getField("DEPATURE_TIME") == null ? null : row.getField("DEPATURE_TIME").toString().substring(0, 10));
                model.setFkDepairport(row.getField("ORIG").toString());
                model.setFkArriairport(row.getField("DEST").toString());

                // 保险

                model.setAkChannel("SCLYX");
                model.setAkInsurstatus(row.getField("INSURE_STATUS") == null? null :
                        NormalizationUtils.standardize(FieldType.INSURANCE_STATUS,
                                row.getField("INSURE_STATUS").toString(), DataSource.LUYAN_TRIP));
                model.setInsuranceAmt(row.getField("INSURE_TAX") == null ? null : Double.parseDouble(row.getField("INSURE_TAX").toString()));
                model.setInsuranceCount(1l);
                model.setInsurancePolicyNo(row.getField("INSURE_SERIAL_NUMBER").toString());


                // 时间
                String updateTime = row.getField("UPDATE_TIME") == null ? row.getField("CREATE_TIME").toString()
                        : row.getField("UPDATE_TIME").toString();
                model.setUpdateTime(updateTime);
                model.setSystemCreatetime(LocalDateTime.now().toString());
                model.setSystemLastUpdatetime(LocalDateTime.now().toString());
                out.collect(model);
            }

        }).returns(AuisSegFactModel.class);

        DorisSink<AuisSegFactModel> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.DWD_DB,
                "T_DWD_AUIS_SEG_FACT",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD);
        modelStream.sinkTo(dorisSink);


    }
}
