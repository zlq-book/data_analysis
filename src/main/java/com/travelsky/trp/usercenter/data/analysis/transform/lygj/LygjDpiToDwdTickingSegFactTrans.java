package com.travelsky.trp.usercenter.data.analysis.transform.lygj;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.trp.utils.DateUtil;
import com.travelsky.dataplatform.utils.DateTimeUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.SM4Utils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingSegFactModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Objects;

import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.*;

/**
 * 鲁雁管家离港数据转DWD出票航段事实表
 */
public class LygjDpiToDwdTickingSegFactTrans {
    //private static final Logger logger = LoggerFactory.getLogger(LygjDpiToDwdTickingSegFactTrans.class);

    /**
     * 数据流处理主方法
     *
     */
    public static void result(DataStream<JSONObject> source) {
        DataStream<TickingSegFactModel> bookingSegStream = source
                .filter(map -> map.getString("ET_NUM") != null && !map.getString("ET_NUM").isEmpty())
                .map(map -> {
                    TickingSegFactModel model = new TickingSegFactModel();
                    String pnr = map.getString("PNR_ICS");
                    String etNum = map.getString("ET_NUM");
                    String fltDate = map.getString("FLT_DATE");
                    String orig = map.getString("ORIG");
                    String dest = map.getString("DEST");
                    String certNo = map.getString("CERT_NO");
                    String cnName = map.getString("PSG_NAME_CN");
                    String enName = map.getString("PSG_NAME_EN");

                    if (StringUtils.isBlank(etNum) || StringUtils.isBlank(orig)) {
                        System.out.println("提取LygjDpiToDwdTickingSegFactTrans失败，etNum为空: " + map.toString());
                        //logger.error("提取LygjDpiToDwdTickingSegFactTrans失败，etNum为空: {}", map.toString());
                        return null;
                    } else {
                        // 票号+起飞机场+起飞日期
                        String fltDateStr = DateUtil.formatToYmd(fltDate);
                        model.setPkId(etNum + orig + fltDateStr);
                        model.setAkTicketNumber(etNum);

                        // 出票信息
                        String printTicketTimeStr = map.getString("PRINT_TICKET_TIME");
                        String printTicketDate = DateUtil.extractDateFromIso(printTicketTimeStr);
                        String printTicketTime = DateUtil.extractTimeFromIso(printTicketTimeStr);
                        model.setFkIssueDate(printTicketDate);
                        // 出票时间
                        model.setFkIssueTime(printTicketTime);
                        // 出票office号
                        model.setAkBookingOfficeNumber(map.getString("ISSUE_OFFI"));
                        // PNR编号
                        model.setAkPnrNumber(pnr);
                        // 起飞机场和到达机场
                        model.setFkDepairport(orig);
                        model.setFkArriairport(dest);


                        // 航程构建 起飞日期 + 承运航司 + 承运航班号 +市场航司+市场航班号+ 起飞机场 + 降落机场
                        String mcFlt = map.getString("MC_FLT");
                        String mcAirlineCode = (mcFlt != null && mcFlt.length() >= 2) ? mcFlt.substring(0, 2) : null;
                        String mcFlightNumber = (mcFlt != null && mcFlt.length() > 2) ? mcFlt.substring(2) : null;
                        String fltFlt = map.getString("FLT_NUM");
                        String fltAirlineCode = (fltFlt != null && fltFlt.length() >= 2) ? fltFlt.substring(0, 2) : null;
                        String fltFlightNumber = (fltFlt != null && fltFlt.length() > 2) ? fltFlt.substring(2) : null;
                        if (StringUtils.isNotBlank(fltDateStr) && StringUtils.isNotBlank(fltAirlineCode)
                                && StringUtils.isNotBlank(fltFlightNumber) && StringUtils.isNotBlank(mcAirlineCode)
                                && StringUtils.isNotBlank(mcFlightNumber) && StringUtils.isNotBlank(orig) && StringUtils.isNotBlank(dest)) {
                            model.setFkSegSegment(fltDateStr +fltAirlineCode + fltFlightNumber + mcAirlineCode + mcFlightNumber + orig + dest);
                        }

                        // 航班日期和时间
                        model.setFkSegDate(DateUtil.extractDateFromIso(fltDate));
                        model.setFkSegTime(DateUtil.extractTimeFromIso(fltDate));

                        String dptm = map.getString("DCS_DPTM");
                        model.setFkSegTime(dptm != null ? dptm + ":00" : null);

                        // 设置TID
                        model.setFkPassengerUserTid(map.getString("TID"));

                        // 乘机人信息
                        String[] result = normalizeName(enName);
                        model.setEnLastName(result[0]);
                        model.setEnFirstName(result[1]);
                        model.setCnName(cnName);
                        model.setCertType(map.getString("ID_TYPE"));
                        model.setCertNumber(certNo);

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
                        // 舱位信息
                        String sellClass = map.getString("SELL_CLASS");
                        model.setAkSegcabin(StringUtils.isNotBlank(sellClass) ? sellClass.substring(0, 1) : null);
                        model.setAkCabin(getCabinClass(model.getFkSegDate(), sellClass));

                        // 常客信息
                        String ffp = map.getString("FFP");
                        if (ffp != null && ffp.length() >= 2) {
                            String ffr = SM4Utils.encrypt(ffp.substring(2), Constants.SM4_KEY);
                            model.setFfrf(ffr);
                            model.setUsedFfr(StringUtils.isNotBlank(ffr));
                            model.setFfAirline(ffp.substring(0, 2));
                        }
                        model.setFfLevel(map.getString("IS_FFP_JK"));

                        // 国内国际标识
                        String ticketType = map.getString("TICKET_TYPE");
                        model.setAkDimark(ticketType);

                        //是否联票
                        model.setAkConjuction(Boolean.valueOf(map.getString("EDI_FLAG")));
                        // 运价基础FB
                        model.setAkFarebasis(map.getString("FARE_BASIS_CODE"));
                        //是否为无陪儿童票
                        model.setUnaccompaniedMinor("1".equals(map.getString("IS_UM")));
                        // 是否政府采购票
                        model.setGovernmentPurchase("1".equals(map.getString("TKNEGP")));
                        // 是否是团队票
                        model.setTeamTicket("Y".equals(map.getString("IS_GROUP")));

                        // 大客户信息
                        String bigCutNum = map.getString("BIG_CUTNUM");
                        model.setAkKeyAccountCode(bigCutNum);
                        model.setKeyAccount(isKeyAccount(bigCutNum));

                        // 同行人数
                        String passengerNum = map.getString("PASSENGER_NUMBER");
                        model.setAkPeerNumber(passengerNum != null ? Integer.parseInt(passengerNum) : 1);
                        model.setSingleTraveler(model.getAkPeerNumber() == 1);
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
                        //是否为自己订票
                        //是否首次购票交易
                        //预订人是否在乘机人列表内

                        // 特殊餐食属性（仅限免费的）
                        model.setAkSpcaAtt(map.getString("SPML_ITEM"));
                        // 预选免费座位号
                        model.setSeatNumber(map.getString("RESERVE_SEAT_NO"));

                        // 提前购票天数 起飞日期 与 出票日期 相差天数。如果日期相同则为0
                        model.setAkAdvbookDay(calculateAdvanceDays(map.getString("PRINT_TICKET_TIME"), map.getString("FLT_DATE"), 0));
                        // 航段提前预订天数区间
                        //model.setAdvbookDayRange();
                        // 季节性标签
                        //model.setSeasonTag(getSeasonTag(map.getString("FLT_DATE")));
                        // 是否为返乡段
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
        DorisSink<TickingSegFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_TICKING_SEG_FACT");
        bookingSegStream.sinkTo(dorisSink).name("DorisSink-BookingSegFact");
    }

}
