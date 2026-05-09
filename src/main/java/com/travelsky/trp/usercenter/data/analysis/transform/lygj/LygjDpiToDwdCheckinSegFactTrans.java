package com.travelsky.trp.usercenter.data.analysis.transform.lygj;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.trp.utils.DateUtil;
import com.travelsky.dataplatform.utils.DateTimeUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.SM4Utils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.CheckinSegFactModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Objects;

import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.*;

/**
 * 鲁雁管家明细事实表-值机-航段级
 * depart_passenger_info这张表里面，CHECK_IN_TIME字段非空的的数据，进入本表
 */
public class LygjDpiToDwdCheckinSegFactTrans {
    //private static final Logger logger = LoggerFactory.getLogger(LygjDpiToDwdCheckinSegFactTrans.class);

    public static void result(DataStream<JSONObject> source) {
        // 2. 转换为DataStream
        // v2 状态为AC的插入
        DataStream<CheckinSegFactModel> checkinSegStream = source
                .filter(map -> null != map.get("CHECK_IN_TIME") && null !=  map.get("STATUS")
                        && "AC".equalsIgnoreCase(map.get("STATUS").toString()))
                .map(map -> {
                    CheckinSegFactModel model = new CheckinSegFactModel();

                    // 主键构建：航班起飞日期+票号+起飞机场+到达机场
                    String fltDate = map.getString("FLT_DATE");
                    String etNum = map.getString("ET_NUM");
                    String orig = map.getString("ORIG");
                    String dest = map.getString("DEST");
                    if (StringUtils.isBlank(fltDate) || StringUtils.isBlank(etNum) ||
                            StringUtils.isBlank(orig) || StringUtils.isBlank(dest)) {
                        System.out.println("提取LygjDpiToDwdCheckinSegFactTrans失败，主键组成部分为空: " + map.toString());
                        //logger.error("提取LygjDpiToDwdCheckinSegFactTrans失败，主键组成部分为空: {}", map.toString());
                        return null;
                    } else {
                        String fltDateYmd = DateUtil.formatToYmd(fltDate);
                        // 票号+起飞机场+起飞日期
                        model.setPkId(etNum + orig + fltDateYmd);

                        // 航班信息
                        String fkSegDate = DateUtil.extractDateFromIso(fltDate);
                        String fkSegTime = DateUtil.extractTimeFromIso(fltDate);
                        model.setFkSegDate(fkSegDate);
                        model.setFkSegTime(fkSegTime);
                        // 票号
                        model.setTikNum(etNum);

                        // 值机日期和时间
                        String checkinTimeStr = map.getString("CHECK_IN_TIME");
                        String checkinDate = DateUtil.extractDateFromIso(checkinTimeStr);
                        String checkinTime = DateUtil.extractTimeFromIso(checkinTimeStr);
                        model.setFkCheckinDate(checkinDate);
                        model.setFkCheckinTime(checkinTime);

                        // 设置TID
                        model.setFkPassengerUserTid(map.getString("TID"));

                        // 乘机人信息
                        String psg_name_en = map.getString("PSG_NAME_EN");
                        String[] result = normalizeName(psg_name_en);
                        model.setEnLastName(result[0]);
                        model.setEnFirstName(result[1]);
                        model.setCnName(map.getString("PSG_NAME_CN"));
                        model.setCertType(map.getString("ID_TYPE"));
                        model.setCertNumber(map.getString("CERT_NO"));

                        // 乘机人年龄计算
                        String birthday = map.getString("BIRTHDAY");
                        // 转成yyyy-MM-dd
                        birthday = DateUtil.convertToStandardFormat(birthday);
                        model.setPassengerAge(getAgeByIdCard(model.getCertNumber(), birthday));
                        // 常客信息
                        String ffp = map.getString("FFP");
                        if (ffp != null && ffp.length() >= 2) {
                            model.setFfAirline(ffp.substring(0, 2));
                            model.setFfrf(SM4Utils.encrypt(ffp.substring(2), Constants.SM4_KEY));

                        }
                        model.setFfLevel(map.getString("IS_FFP_JK"));


                        // 航程构建 起飞日期 + 承运航司 + 承运航班号 +市场航司+市场航班号+ 起飞机场 + 降落机场
                        // 市场航司和航班号
                        String mcFlt = map.getString("MC_FLT");
                        String mcAirlineCode = (mcFlt != null && mcFlt.length() >= 2) ? mcFlt.substring(0, 2) : null;
                        String mcFlightNumber = (mcFlt != null && mcFlt.length() > 2) ? mcFlt.substring(2) : null;
                        // 承运航司
                        String fltFlt = map.getString("FLT_NUM");
                        String fltAirlineCode = (fltFlt != null && fltFlt.length() >= 2) ? fltFlt.substring(0, 2) : null;
                        String fltFlightNumber = (fltFlt != null && fltFlt.length() > 2) ? fltFlt.substring(2) : null;
                        if (StringUtils.isNotBlank(fltDateYmd) && StringUtils.isNotBlank(fltAirlineCode)
                                && StringUtils.isNotBlank(fltFlightNumber) && StringUtils.isNotBlank(mcAirlineCode)
                                && StringUtils.isNotBlank(mcFlightNumber) && StringUtils.isNotBlank(orig) && StringUtils.isNotBlank(dest)) {
                            model.setFkCheckinSeg(fltDateYmd +fltAirlineCode + fltFlightNumber + mcAirlineCode + mcFlightNumber + orig + dest);
                        }

                        String dptm = map.getString("DCS_DPTM");
                        model.setFkSegTime(dptm != null ? dptm + ":00" : null);

                        model.setFkDepairport(orig);
                        model.setFkArriairport(dest);
                        // 国内国际标识
                        String ticketType = map.getString("TICKET_TYPE");
                        model.setAkDimark(ticketType);

                        // 值机信息
                        model.setAkCheckinPid(map.getString("CKI_PID"));
                        model.setCheckinAgent(map.getString("CKI_AGENT"));
                        // 座位号
                        model.setAkSeatAssign(map.getString("SEAT_NO"));

                        // 舱位信息
                        String sellClass = map.getString("SELL_CLASS");
                        model.setAkSegcabin(StringUtils.isNotBlank(sellClass) ? sellClass.substring(0, 1) : null);
                        // 舱等
                        model.setAkCabin(getCabinClass(model.getFkSegDate(), sellClass));

                        // 值机状态和方式
                        model.setAkCheckinStatus(map.getString("CKI_STATUS"));
                        model.setAkCheckinType(map.getString("CKI_TYPE"));
                        model.setCheckinChannel(convertAgentToCode(map.getString("CKI_AGENT")));

                        // 行李信息
                        String bags = map.getString("BAGS");
                        int bagCount = bags != null ? Integer.parseInt(bags) : 0;
                        model.setAkBaggageFlag(bagCount > 0);
                        model.setSegBaggagecount(bagCount);

                        String bagWeight = map.getString("BAGWHT");
                        if (bagWeight != null) {
                            try {
                                model.setSegBaggageweight(Double.parseDouble(bagWeight));
                            } catch (NumberFormatException e) {
                                //logger.warn("行李重量转换失败: {}", bagWeight);
                                System.out.println("行李重量转换失败: " + bagWeight);
                            }
                        }
                        if (StringUtils.isNotBlank(checkinTimeStr) && StringUtils.isNotBlank(fkSegDate) && StringUtils.isNotBlank(model.getFkSegTime())) {
                            // 提前值机时间 （航班起飞日期-值机日期）*24+（起飞时间小时数-值机时间小时数）
                            model.setEarlyCheckintime((long) calculateEarlyCheckinHours(DateUtil.isoToTraditional(checkinTimeStr), fkSegDate, model.getFkSegTime()));
                        }

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
        DorisSink<CheckinSegFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_CHECKIN_SEG_FACT");
        checkinSegStream.sinkTo(dorisSink).name("DorisSink-CheckinSegFact");
    }

}
