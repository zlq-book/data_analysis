package com.travelsky.trp.usercenter.data.analysis.transform.lygj;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.trp.utils.DateUtil;
import com.travelsky.dataplatform.utils.DateTimeUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.BookingPnrFactModel;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Objects;

/**
 * * 机票预订-PNR级
 */
public class LygjDpiToDwdBookingPnrFactTrans {
    //private static final Logger logger = LoggerFactory.getLogger(LygjDpiToDwdBookingPnrFactTrans.class);


    public static void result(DataStream<JSONObject> source) {
        // 4. 将Map数据流转换为BookingPnrFactModel流
        SingleOutputStreamOperator<BookingPnrFactModel> bookingPnrFactStream = source
                .map(map -> {
                    BookingPnrFactModel model = new BookingPnrFactModel();
                    // 年龄计算
                    Integer age = null; // 默认值（可根据业务需求调整）
                    try {
                        String birthday = map.getString("BIRTHDAY");
                        // 转成yyyy-MM-dd
                        birthday = DateUtil.convertToStandardFormat(birthday);
                        age = TransUtils.getAgeByIdCard(map.getString("CERT_NO"), birthday);
                    } catch (NumberFormatException e) {
                        //logger.error("乘客年龄格式错误: {}", map.toString(), e);
                        age = null; // 解析失败时使用默认值
                    }

                    // 主键ID = PNR号+创建日期(YYYYMMDD)
                    String pnr = map.getString("PNR_ICS");
                    String createTime = map.getString("CREATE_TIME");
                    if (StringUtils.isBlank(pnr) || StringUtils.isBlank(createTime)) {
                        System.out.println("提取LygjDpiToDwdBookingPnrFactTrans失败，pnr为空或者创建时间为空:"+map.toString());
                        //logger.error("提取LygjDpiToDwdBookingPnrFactTrans失败，{}", map.toString());
                        return null;
                    } else {

                        model.setPkId(pnr + createTime.substring(0, 10).replace("-", ""));
                        String recordCreateTime = map.getString("RECORD_CREATE_TIME");
                        // 基础PNR信息
                        model.setPnrNumber(pnr);
                        model.setFkPnrcreateDate(DateUtil.extractDateFromIso(recordCreateTime));
                        model.setFkPnrcreateTime(DateUtil.extractTimeFromIso(recordCreateTime));

                        // GDS和订座信息
                        model.setGdsCode(map.getString("GDS_CODE"));
                        model.setAkBookingOffice(map.getString("BOOK_OFFI"));
                        model.setAkTeammark("Y".equals(map.getString("IS_GROUP")));

                        // 大客户信息
                        String bigCutNum = map.getString("BIG_CUTNUM");
                        model.setAkKeyAccountCode(bigCutNum);
                        model.setKeyAccount(Boolean.valueOf(map.getString("IS_KEY_ACCOUNT")));

                        // 同行人数
                        String peerNumStr = map.getString("PASSENGER_NUMBER");
                        Integer peerNum = peerNumStr != null ? Integer.parseInt(peerNumStr) : null;
                        model.setAkPeerNumber(peerNum);

                        // 是否单人出行
                        model.setSingleTravel(peerNum != null && peerNum == 1);

                        //根据“机票预订-航段表”的计算结果，反向往这个表写。
                        // 是否与儿童同行
                        model.setPeerChd("CHD".equals(map.getString("USER_TYPE")));
                        //是否与老人同行
                        if (null != age && age > 65) {
                            model.setPeerSenior(true);
                        } else {
                            model.setPeerSenior(false);
                        }
                        //是否与婴儿同行
                        //model.setPeerChd(null);
                        //预订人是否在乘机人列表
                        // model.setAkBookerIsPassenger(null);

                        // 设置TID
                        model.setFkBookingUserTid(map.getString("TID"));

                        // 系统信息
                        String now = DateTimeUtils.getCurrentDateTime();
                        model.setSystemCreatetime(now);

                        // 是否启用
                        model.setDataActive(true);
                        // 启用时间
                        model.setDataActiveTime(now);

                        return model;
                    }
                }).setParallelism(4)
                .filter(Objects::nonNull);

        // 5. 创建Doris Sink并写入数据
        DorisSink<BookingPnrFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_BOOKING_PNR_FACT");

        bookingPnrFactStream.sinkTo(dorisSink);
    }
}
