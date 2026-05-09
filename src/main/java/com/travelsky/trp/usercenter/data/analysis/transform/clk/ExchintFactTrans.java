package com.travelsky.trp.usercenter.data.analysis.transform.clk;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.MileRedemptionFactModal;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ExchintFactTrans {

    static final Logger logger = LoggerFactory.getLogger(ExchintFactTrans.class);

    public static void result(StreamTableEnvironment tEnv, String etlDate) {
        //创建Doris目标表
        tEnv.executeSql(CreateTableSql.CLK_EXCHINT);

        // 从 Doris 表中读取数据
        Table dorisTable = tEnv.sqlQuery("SELECT " +
                "ID,\n" +
                "DOCNAME,\n" +
                "TOTALRECORD,\n" +
                "TOTALAMOUNT,\n" +
                "TOTALVALUE,\n" +
                "BILLINGMONTH,\n" +
                "BILLINGSTARTDT,\n" +
                "BILLINGENDDT,\n" +
                "MEMBERNO,\n" +
                "MEMBERNO TID,\n" +
                "MEMBERTIERCODE,\n" +
                "MEMBERBRAND,\n" +
                "BIZTYPECODE,\n" +
                "BIZSUBTYPECODE,\n" +
                "EXCHCHANNEL,\n" +
                "PARTNERCODE,\n" +
                "EXCHNO,\n" +
                "ACTIVITYID,\n" +
                "TRANID,\n" +
                "ORDERNO,\n" +
                "TKTNO,\n" +
                "COUPONNO,\n" +
                "BILLCOMPANY,\n" +
                "BILLEDCOMPANY,\n" +
                "MILES,\n" +
                "`VALUE`,\n" +
                "TPM,\n" +
                "CURRENCY,\n" +
                "SALESPRICE,\n" +
                "COSTSPRICE,\n" +
                "PNR,\n" +
                "EXCHDATE,\n" +
                "FLIGHTDATE,\n" +
                "OC,\n" +
                "OCFLIGHTNO,\n" +
                "OCCABIN,\n" +
                "OCSUBCLASS,\n" +
                "BUSUBCLASS,\n" +
                "UPLSTN,\n" +
                "DESSTN,\n" +
                "COMMODITYNO,\n" +
                "PRODCUINFO,\n" +
                "ASS " +
                " FROM T_ODS_CLK_EXCHINT "
                + " WHERE  ETL_DATE='" + etlDate + "'");

        DataStream<Row> rowDataStream = tEnv.toDataStream(dorisTable);
        // 后续可以进行 map、filter、sink 操作
        SingleOutputStreamOperator<MileRedemptionFactModal> registerUdoFactModelStream = rowDataStream.map(row -> {
            MileRedemptionFactModal model = new MileRedemptionFactModal();
            String docName = Objects.toString(row.getField("DOCNAME"), "");
            String exchNo = Objects.toString(row.getField("EXCHNO"), "");
            String activityId = Objects.toString(row.getField("ACTIVITYID"), "");

            String result = docName + exchNo + activityId + "EXCHINT";
            model.setPkId(result);
            // 数据源
            model.setMileageType("EXCHINT");
            // 公共部分 1-7
            model.setFileName(row.getField("DOCNAME").toString());
            model.setTotalRecords(Integer.parseInt(row.getField("TOTALRECORD").toString()));
            model.setTotalMiles(BigDecimal.valueOf(Double.parseDouble(row.getField("TOTALAMOUNT").toString())));
            model.setTotalAmount(BigDecimal.valueOf(Double.parseDouble(row.getField("TOTALVALUE").toString())));
            model.setBillingMonth(row.getField("BILLINGMONTH").toString());
            model.setBillingStartDate(row.getField("BILLINGSTARTDT").toString());
            model.setBillingEndDate(row.getField("BILLINGENDDT").toString());
            // list部分 8-20
            model.setMemberNumber(row.getField("MEMBERNO") == null ? null : row.getField("MEMBERNO").toString());
            model.setMemberLevel(row.getField("MEMBERTIERCODE") == null ? null : row.getField("MEMBERTIERCODE").toString());
            model.setMemberBrand(row.getField("MEMBERBRAND").toString());
            model.setBizType(row.getField("BIZTYPECODE").toString());
            model.setBizSubtype(row.getField("BIZSUBTYPECODE").toString());
            model.setChannelCode(row.getField("EXCHCHANNEL") == null ? null : row.getField("EXCHCHANNEL").toString());
            model.setPartnerCode(row.getField("PARTNERCODE").toString());
            model.setEventNumber(row.getField("EXCHNO") == null ? null : row.getField("EXCHNO").toString());
            model.setActivityId(row.getField("ACTIVITYID").toString());
            model.setTransactionId(row.getField("TRANID") == null ? null : row.getField("TRANID").toString());
            model.setOrderNo(row.getField("ORDERNO") == null ? null : row.getField("ORDERNO").toString());
            model.setTicketNo(row.getField("TKTNO") == null ? null : row.getField("TKTNO").toString());
            model.setCouponNo(row.getField("COUPONNO") == null ? null :row.getField("COUPONNO").toString());
            // list部分 21-30
            model.setBillCompany(row.getField("BILLCOMPANY").toString());
            model.setBilledCompany(row.getField("BILLEDCOMPANY").toString());
            model.setMilesAccumulated(row.getField("MILES") == null ? null :
                    BigDecimal.valueOf(Double.parseDouble(row.getField("MILES").toString())));
            model.setMileageValue(BigDecimal.valueOf(Double.parseDouble(row.getField("VALUE").toString())));
            model.setTpm(row.getField("TPM") == null ? null : row.getField("TPM").toString());
            model.setCurrency(row.getField("CURRENCY").toString());
            model.setSalesAmount(BigDecimal.valueOf(Double.parseDouble(row.getField("SALESPRICE").toString())));
            model.setCostAmount(row.getField("COSTSPRICE") == null ? null :
                    BigDecimal.valueOf(Double.parseDouble(row.getField("COSTSPRICE").toString())));
            model.setPnr(row.getField("PNR") == null ? null : row.getField("PNR").toString());
            model.setExchDate(row.getField("EXCHDATE") == null ? null : row.getField("EXCHDATE").toString());
            // list部分 31-41
            model.setFlightDate(row.getField("FLIGHTDATE") == null ? null : row.getField("FLIGHTDATE").toString());
            model.setOcCarrier(row.getField("OC") == null ? null : row.getField("OC").toString());
            model.setOcFlightNo(row.getField("OCFLIGHTNO") == null ? null : row.getField("OCFLIGHTNO").toString());
            model.setOcCabin(row.getField("OCCABIN") == null ? null : row.getField("OCCABIN").toString());
            model.setOcSubcabin(row.getField("OCSUBCLASS") == null ? null : row.getField("OCSUBCLASS").toString());
            model.setOriginalSubcabin(row.getField("BUSUBCLASS") == null ? null : row.getField("BUSUBCLASS").toString());
            model.setOriginStation(row.getField("UPLSTN") == null ? null : row.getField("UPLSTN").toString());
            model.setDestinationStation(row.getField("DESSTN") == null ? null : row.getField("DESSTN").toString());
            model.setProductId(row.getField("COMMODITYNO") == null ? null : row.getField("COMMODITYNO").toString());
            model.setProductInfo(row.getField("PRODCUINFO") == null ? null : row.getField("PRODCUINFO").toString());
            model.setProfitSharing(row.getField("ASS") == null ? null : row.getField("ASS").toString());
            // 舱等不需要
            // list部分
            model.setExchCount(1);
            // TID
            model.setCumulationTid(TransUtils.getClkTid(row.getField("MEMBERNO").toString()));
            // 入库时间
            model.setSystemCreatetime(LocalDateTime.now().toString());
            model.setSystemLastUpdatetime(LocalDateTime.now().toString());
            return model;
        });

        DorisSink<MileRedemptionFactModal> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.DWD_DB,
                "T_DWD_MILE_REDEMPTION_FACT",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD);
        registerUdoFactModelStream.sinkTo(dorisSink);

    }
}
