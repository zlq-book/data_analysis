package com.travelsky.trp.usercenter.data.analysis.transform.lygj;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.trp.utils.DateUtil;
import com.travelsky.dataplatform.utils.DateTimeUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.SM4Utils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.RefundSegFactModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Objects;

import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.*;

/**
 * *机票退票-航段级
 */
public class LygjDpiToDwdRefundSegFactTrans {
    //private static final Logger logger = LoggerFactory.getLogger(LygjDpiToDwdRefundSegFactTrans.class);

    public static void result(DataStream<JSONObject> source) {
        DataStream<RefundSegFactModel> refundSegStream = source
                .filter(map -> "R".equals(map.getString("COUPON_STATUS")))
                .map(map -> {
                    RefundSegFactModel model = new RefundSegFactModel();
                    String fltDate = map.getString("FLT_DATE");
                    String etNum = map.getString("ET_NUM");
                    String orig = map.getString("ORIG");
                    String dest = map.getString("DEST");

                    if (StringUtils.isBlank(etNum) || StringUtils.isBlank(orig)) {
                        System.out.println("提取LygjDpiToDwdRefundSegFactTrans失败，etNum为空: " + map.toString());
                        //logger.error("提取LygjDpiToDwdRefundSegFactTrans失败，etNum为空: {}", map.toString());
                        return null;
                    } else {
                        String fltDateYmd = DateUtil.formatToYmd(fltDate);
                        // 票号+起飞机场+起飞日期
                        model.setPkId(etNum + orig + fltDateYmd);

                        // PNR信息
                        model.setAkTikNumber(etNum);
                        model.setFkDepairport(orig);
                        model.setFkArriairport(dest);

                        // 出票信息
                        String printTicketTimeStr = map.getString("PRINT_TICKET_TIME");
                        String printTicketDate = DateUtil.extractDateFromIso(printTicketTimeStr);
                        String printTicketTime = DateUtil.extractTimeFromIso(printTicketTimeStr);
                        model.setTicketingDate(printTicketDate);
                        model.setTicketingTime(printTicketTime);

                        // 航班信息
                        model.setFkSegDate(DateUtil.extractDateFromIso(fltDate));

                        // 航程构建
                        String mcFlt = map.getString("MC_FLT");
                        String mcAirlineCode = (mcFlt != null && mcFlt.length() >= 2) ? mcFlt.substring(0, 2) : null;
                        String mcFlightNumber = (mcFlt != null && mcFlt.length() > 2) ? mcFlt.substring(2) : null;
                        String fltFlt = map.getString("FLT_NUM");
                        String fltAirlineCode = (fltFlt != null && fltFlt.length() >= 2) ? fltFlt.substring(0, 2) : null;
                        String fltFlightNumber = (fltFlt != null && fltFlt.length() > 2) ? fltFlt.substring(2) : null;
                        if (StringUtils.isNotBlank(fltDateYmd) && StringUtils.isNotBlank(fltAirlineCode)
                                && StringUtils.isNotBlank(fltFlightNumber) && StringUtils.isNotBlank(mcAirlineCode)
                                && StringUtils.isNotBlank(mcFlightNumber) && StringUtils.isNotBlank(orig) && StringUtils.isNotBlank(dest)) {
                            model.setFkSegSegment(fltDateYmd +fltAirlineCode + fltFlightNumber + mcAirlineCode + mcFlightNumber + orig + dest);
                        }

                        String dptm = map.getString("DCS_DPTM");
                        model.setFkSegTime(dptm != null ? dptm + ":00" : null);

                        // 设置TID
                        model.setFkPassengerUserTid(map.getString("TID"));

                        // 乘机人信息
                        model.setCnName(map.getString("PSG_NAME_CN"));
                        String psg_name_en = map.getString("PSG_NAME_EN");
                        String[] result = normalizeName(psg_name_en);
                        model.setEnLastName(result[0]);
                        model.setEnFirstName(result[1]);
                        model.setCertType(map.getString("ID_TYPE"));
                        model.setCertNumber(map.getString("CERT_NO"));

                        // 年龄计算
                        Integer age = null; // 默认值（可根据业务需求调整）
                        try {
                            String birthday = map.getString("BIRTHDAY");
                            // 转成yyyy-MM-dd
                            birthday = DateUtil.convertToStandardFormat(birthday);
                            age = getAgeByIdCard(model.getCertNumber(), birthday);
                        } catch (NumberFormatException e) {
                            //logger.error("乘客年龄格式错误: {}", map.toString(), e);
                            age = null; // 解析失败时使用默认值
                        }
                        model.setPassengerAge(age);
                        // v2.0
//                        model.setPassengerType(map.getString("USER_TYPE"));
//
//                        //是否与儿童同行
//                        model.setAkPeerChd("CHD".equals(map.getString("USER_TYPE")));
                        // 是否与老人同行 2.0增加
//                        if (null != model.getPassengerAge() && model.getPassengerAge() > 65) {
//                            model.setPeerSenior(true);
//                        } else {
//                            model.setPeerSenior(false);
//                        }

                        // 舱位信息
                        String sellClass = map.getString("SELL_CLASS");
                        model.setAkSegcabin(StringUtils.isNotBlank(sellClass) ? sellClass.substring(0, 1) : null);
                        model.setAkCabin(getCabinClass(model.getFkSegDate(), sellClass));


                        // 大客户信息
                        String bigCutNum = map.getString("BIG_CUTNUM");
                        model.setKeyAccount(isKeyAccount(bigCutNum));

                        // 常客信息
                        String paxFfp = map.getString("FFP");
                        if (paxFfp != null && paxFfp.length() >= 2) {
                            model.setFfAirline(paxFfp.substring(0, 2));
                            model.setFfrf(SM4Utils.encrypt(paxFfp.substring(2), Constants.SM4_KEY));
                        }
                        model.setFfLevel(map.getString("IS_FFP_JK"));

                        // 国内国际标识
                        String ticketType = map.getString("TICKET_TYPE");
                        model.setAkDimark( ticketType);

                        // 系统时间
                        String now = DateTimeUtils.getCurrentDateTime();
                        model.setSystemCreatetime(now);

                        model.setDataActive(true);
                        model.setDataActiveTime(now);
                        return model;
                    }
                }).setParallelism(4)
                .filter(Objects::nonNull);

        // 写入Doris
        DorisSink<RefundSegFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_REFUND_SEG_FACT");
        refundSegStream.sinkTo(dorisSink).name("DorisSink-RefundSegFact");
    }
}
