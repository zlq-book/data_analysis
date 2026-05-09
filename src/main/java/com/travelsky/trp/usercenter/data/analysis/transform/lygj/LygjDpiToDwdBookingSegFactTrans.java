package com.travelsky.trp.usercenter.data.analysis.transform.lygj;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.trp.utils.DateUtil;
import com.travelsky.dataplatform.utils.DateTimeUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.SM4Utils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.BookingSegFactModel;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Objects;

import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.getCabinClass;
import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.normalizeName;

/**
 * * 机票预订-航段级
 */
public class LygjDpiToDwdBookingSegFactTrans {
    //private static final Logger logger = LoggerFactory.getLogger(LygjDpiToDwdBookingSegFactTrans.class);

    public static void result(DataStream<JSONObject> source) {
        // 4. 将Map数据流转换为BookingSegFactModel流
        SingleOutputStreamOperator<BookingSegFactModel> bookingSegStream = source
                .map(map -> {
                    // 主键构建
                    // PNR编号+起飞机场+起飞日期+起飞时间+乘机人证件号（加密）+乘机人姓名（姓名，先取乘机人中文姓名，如果没有，取英文姓+英文名）---备注：姓名不带斜杠不带空格。
                    BookingSegFactModel model = new BookingSegFactModel();
                    String pnr = map.getString("PNR_ICS");
                    String orig = map.getString("ORIG");
                    String fltDate = map.getString("FLT_DATE");
                    String segDate = DateUtil.formatToYmd(fltDate);
                    String dptm = StringUtils.isNotBlank(map.getString("DCS_DPTM")) ? map.getString("DCS_DPTM") + ":00" : null;
                    String certNo = map.getString("CERT_NO");
                    String psgNameCn = map.getString("PSG_NAME_CN");
                    String nameKey = psgNameCn;
                    String psg_name_en = map.getString("PSG_NAME_EN");
                    String[] result = normalizeName(psg_name_en);
                    model.setEnLastName(result[0]);
                    model.setEnFirstName(result[1]);
                    if (StringUtils.isBlank(psgNameCn)) {
                        nameKey = result[0] + result[1];
                    }
                    model.setCnName(psgNameCn);
                    if (StringUtils.isBlank(pnr) || StringUtils.isBlank(orig)) {
                        System.out.println("提取LygjDpiToDwdBookingSegFactTrans失败，主键组成部分为空: " + map.toString());
                        //logger.error("提取LygjDpiToDwdBookingSegFactTrans失败，主键组成部分为空: {}", map.toString());
                        return null;
                    } else {
                        String createTime = map.getString("RECORD_CREATE_TIME");
                        String dest = map.getString("DEST");
                        String pkId = pnr + orig + segDate + dptm + certNo + nameKey;
                        model.setPkId(pkId);

                        // PNR信息
                        model.setPnrNumber(pnr);

                        model.setFkBookingDate(DateUtil.extractDateFromIso(createTime));
                        model.setFkBookingTime(DateUtil.extractTimeFromIso(createTime));

                        String fkSegDate = DateUtil.extractDateFromIso(fltDate);
                        model.setFkSegDate(fkSegDate);
                        // 机场和航班信息
                        model.setFkDepairport(orig);
                        model.setFkArriairport(dest);
                        model.setFkSegTime(dptm);
                        // 航程主键：起飞日期 + 承运航司 + 承运航班号 +市场航司+市场航班号+ 起飞机场 + 降落机场
                        String mcFlt = map.getString("MC_FLT");
                        String mcAirlineCode = (mcFlt != null && mcFlt.length() >= 2) ? mcFlt.substring(0, 2) : null;
                        String mcFlightNumber = (mcFlt != null && mcFlt.length() > 2) ? mcFlt.substring(2) : null;

                        String fltFlt = map.getString("FLT_NUM");
                        String fltAirlineCode = (fltFlt != null && fltFlt.length() >= 2) ? fltFlt.substring(0, 2) : null;
                        String fltFlightNumber = (fltFlt != null && fltFlt.length() > 2) ? fltFlt.substring(2) : null;

                        if (StringUtils.isNotBlank(segDate) && StringUtils.isNotBlank(fltAirlineCode)
                            && StringUtils.isNotBlank(fltFlightNumber) && StringUtils.isNotBlank(mcAirlineCode)
                            && StringUtils.isNotBlank(mcFlightNumber) && StringUtils.isNotBlank(orig) && StringUtils.isNotBlank(dest)) {
                            model.setFkBookingSeg(segDate +fltAirlineCode + fltFlightNumber + mcAirlineCode + mcFlightNumber + orig + dest);
                        }


                        // 设置TID
                        model.setFkBookingUserTid(map.getString("TID"));

                        // 乘机人信息
                        model.setCnName(map.getString("PSG_NAME_CN"));
                        model.setCertType(map.getString("ID_TYPE"));
                        model.setCertNumber(certNo);

                        // 年龄计算
                        Integer age = null; // 默认值（可根据业务需求调整）
                        try {
                            String birthday = map.getString("BIRTHDAY");
                            // 转成yyyy-MM-dd
                            birthday = DateUtil.convertToStandardFormat(birthday);
                            age = TransUtils.getAgeByIdCard(model.getCertNumber(), birthday);
                        } catch (NumberFormatException e) {
                            //logger.error("乘客年龄格式错误: {}", map.toString(), e);
                            age = null; // 解析失败时使用默认值
                        }
                        model.setPassengerAge(age);

                        // VIP标识
                        model.setVvip(map.getString("IS_VVIP"));

                        // 订座信息
                        model.setAkBookingOfficeNumber(map.getString("BOOK_OFFI"));
                        model.setAkTeammark("Y".equals(map.getString("IS_GROUP")));
                        model.setAkSegStatus(map.getString("LKT_STATUS"));

                        // 舱位信息
                        String sellClass = map.getString("SELL_CLASS");
                        model.setAkSegcabin(StringUtils.isNotBlank(sellClass) ? sellClass.substring(0, 1) : null);
                        model.setAkCabin(getCabinClass(model.getFkSegDate(), sellClass));
                        // 票价类型（全价票、折扣票、特价票） v2做
                        //"根据舱等以及折扣这2个字段来计算：
                        //全价票（FL）：公务、高经、经济舱的全价票
                        //折扣票（DF）：高经和公务舱的折扣票、经济舱（4折以上）
                        //特价票（SP）：经济舱，4折以下票"
                        //model.setAkTicketType(TransUtils.getTicketType(row.getFieldAs("DISCOUNT"), sellClass));


                        // 常客信息
                        String ffp = map.getString("FFP");
                        if (ffp != null && ffp.length() >= 2) {
                            model.setFfAirline(ffp.substring(0, 2));
                            model.setFfrf(SM4Utils.encrypt(ffp.substring(2), Constants.SM4_KEY));
                        }
                        model.setFfLevel(map.getString("IS_FFP_JK"));


                        if (StringUtils.isNotBlank(createTime) && StringUtils.isNotBlank(fltDate)){
                            // 提前预订天数（航段级） 航班日期 与 预订日期相差天数，如果日期相同则为0。
                            model.setAkAdvbookDay(TransUtils.calculateAdvanceDays(createTime.substring(0, 10), fltDate, 0));
                        }

                        // 同行人数
                        String passengerNumber = map.getString("PASSENGER_NUMBER");
                        Integer peerNum = passengerNumber != null ? Integer.parseInt(passengerNumber) : null;
                        model.setAkPeerNumber(peerNum);

                        // 大客户信息
                        String bigCutNum = map.getString("BIG_CUTNUM");
                        model.setAkKeyAccountCode(bigCutNum);
                        model.setKeyAccount(Boolean.valueOf(map.getString("IS_KEY_ACCOUNT")));
                        model.setPassengerType(map.getString("USER_TYPE"));

                        //是否与儿童同行
                        model.setAkPeerChd("CHD".equals(map.getString("USER_TYPE")));
                        //是否与老人同行
                        if (null != model.getPassengerAge() && model.getPassengerAge() > 65) {
                            model.setPeerSenior(true);
                        } else {
                            model.setPeerSenior(false);
                        }
                        //是否与婴儿同行
                        //model.setPeerChd(null);
                        //季节性标签
                        //model.setSeasonTag(TransUtils.getSeasonTag(map.getString("FLT_DATE")));
                        //是否为自己订票
                        //是否为返乡段
                        //是否单人出行
                        model.setSingleTravel(peerNum != null && peerNum == 1);
                        //预订人是否在乘机人列表

                        // 飞行时是否使用常客卡
                        model.setUsedFfr(StringUtils.isNotBlank(ffp));
                        // 系统信息
                        String now = DateTimeUtils.getCurrentDateTime();
                        model.setSystemCreatetime(now);
                        model.setDataActive(true);
                        model.setDataActiveTime(now);

                        return model;
                    }
                }).setParallelism(4)
                .filter(Objects::nonNull);

        // 3. 写入Doris
        DorisSink<BookingSegFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_BOOKING_SEG_FACT");
        bookingSegStream.sinkTo(dorisSink).name("DorisSink-BookingSegFact");
    }
}
