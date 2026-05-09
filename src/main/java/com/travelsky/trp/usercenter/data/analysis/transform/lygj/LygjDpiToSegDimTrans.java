package com.travelsky.trp.usercenter.data.analysis.transform.lygj;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.trp.utils.DateUtil;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.SegDimModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.time.LocalDateTime;
import java.util.Objects;

public class LygjDpiToSegDimTrans {

    //private static final Logger logger = LoggerFactory.getLogger(LygjDpiToSegDimTrans.class);

    public static void result(DataStream<JSONObject> source) {
        SingleOutputStreamOperator<SegDimModel> bookingPnrFactStream = source
                .map(map -> {

                    SegDimModel model = new SegDimModel();
                    String fltDate = map.getString("FLT_DATE");
                    String departureDate = DateUtil.extractDateFromIso(fltDate);
                    String departureTime = map.getString("DCS_DPTM");
                    String fltDateYmd = DateUtil.formatToYmd(fltDate);

                    // 航班时间信息
                    model.setDepartureDate(departureDate);
                    model.setDepartureTime(StringUtils.isNotBlank(departureTime) ? departureTime + ":00" : null);

                    String dcsAtdate = map.getString("DCS_ATDATE");
                    String dcsDate = DateUtil.extractDateFromIso(dcsAtdate);
                    String dcsTime = map.getString("DCS_ATTM");

                    model.setArrivalDate(dcsDate);
                    model.setArrivalTime(StringUtils.isNotBlank(dcsTime) ? dcsTime + ":00" : null);

                    // 市场航司信息
                    String mcFlt = map.getString("MC_FLT");
                    String mcAirlineCode = (mcFlt != null && mcFlt.length() >= 2) ? mcFlt.substring(0, 2) : null;
                    String mcFlightNumber = (mcFlt != null && mcFlt.length() > 2) ? mcFlt.substring(2) : null;
                    model.setMarketAirline(mcAirlineCode);
                    model.setMarketFlightNum(mcFlightNumber);

                    // 承运航司信息
                    String fltFlt = map.getString("FLT_NUM");
                    String fltAirlineCode = (fltFlt != null && fltFlt.length() >= 2) ? fltFlt.substring(0, 2) : null;
                    String fltFlightNumber = (fltFlt != null && fltFlt.length() > 2) ? fltFlt.substring(2) : null;
                    model.setOperatAirline(fltAirlineCode);
                    model.setOperatFlightNum(fltFlightNumber);

                    String orig = map.getString("ORIG");
                    String dest = map.getString("DEST");

                    // 主键ID
                    if (StringUtils.isNotBlank(fltDateYmd) && StringUtils.isNotBlank(fltAirlineCode)
                            && StringUtils.isNotBlank(fltFlightNumber) && StringUtils.isNotBlank(mcAirlineCode)
                            && StringUtils.isNotBlank(mcFlightNumber) && StringUtils.isNotBlank(orig) && StringUtils.isNotBlank(dest)) {
                        model.setSegmentKey(fltDateYmd + fltAirlineCode + fltFlightNumber + mcAirlineCode + mcFlightNumber + orig + dest);
                    } else {
                        System.out.println("提取LygjDpiToSegDimTrans失败，主键组成部分为空: " + map.toString());
                        //logger.error("提取LygjDpiToSegDimTrans失败，主键组成部分为空{}", map.toString());
                        return null;
                    }
                    // 航线
                    model.setAirline(orig + "_" + dest);
                    // 是否代码共享航班

                    model.setCreateTime(LocalDateTime.now().toString());
                    model.setUpdateTime(LocalDateTime.now().toString());
                    // 飞行时长 要使用从FOC拿的东八区时间，不能用高频解析的时间。
                    // 如果起飞日期=到达日期：降落时间-起飞时间的分钟数。
                    // 如果起飞日期<到达日期：（24点-起飞时间）+（降落时间-0点）
                    //model.setDuration();

                    // 机型
                    //model.setAirtype();
                    return model;
                }).setParallelism(4)
                .filter(Objects::nonNull);

        // 写入Doris
        DorisSink<SegDimModel> dorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_SEG_DIM");
        bookingPnrFactStream.sinkTo(dorisSink);
    }
}
