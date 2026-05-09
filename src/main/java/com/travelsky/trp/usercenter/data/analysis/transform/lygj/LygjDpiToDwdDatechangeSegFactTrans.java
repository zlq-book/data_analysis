package com.travelsky.trp.usercenter.data.analysis.transform.lygj;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.trp.utils.DateUtil;
import com.travelsky.dataplatform.utils.DateTimeUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.SM4Utils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.DatechangeSegFactModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Objects;

import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.*;

/**
 * * 机票改升换开出票-航段级
 */
public class LygjDpiToDwdDatechangeSegFactTrans {
    //private static final Logger logger = LoggerFactory.getLogger(LygjDpiToDwdDatechangeSegFactTrans.class);

    public static void result(DataStream<JSONObject> source) {
        // 2. 转换为DataStream
        DataStream<DatechangeSegFactModel> datechangeSegStream = source
                .filter(map -> map.getString("ORIGINALTKNE") != null && !map.getString("ORIGINALTKNE").isEmpty())
                .map(map -> {
                    DatechangeSegFactModel model = new DatechangeSegFactModel();

                    // 主键ID：改升后票号+起飞机场+起飞日期
                    String printTimeStr = map.getString("PRINT_TICKET_TIME");
                    String etNum = map.getString("ET_NUM");
                    String orig = map.getString("ORIG");
                    String dest = map.getString("DEST");
                    String fltDate = map.getString("FLT_DATE");
                    if (StringUtils.isBlank(etNum) || StringUtils.isBlank(orig)) {
                        System.out.println("提取LygjDpiToDwdDatechangeSegFactTrans失败，主键部分组成为空: " + map.toString());
                        //logger.error("提取LygjDpiToDwdDatechangeSegFactTrans失败，主键部分组成为空: {}", map.toString());
                        return null;
                    } else {
                        String fltDateYmd = DateUtil.formatToYmd(fltDate);
                        model.setPkId(etNum + orig + fltDateYmd);

                        // PNR编号
                        String pnr = map.getString("PNR_ICS");
                        model.setAkPnrNumber(pnr);

                        // 票号信息
                        model.setNewTikNum(etNum);
                        model.setOriTikNum(map.getString("ORIGINALTKNE"));

                        // 出票office号
                        model.setAkBookingOfficeNumber(map.getString("ISSUE_OFFI"));

                        // 换开出票日期和时间
                        String printDate = DateUtil.extractDateFromIso(printTimeStr);
                        model.setFkExchangeDate(printDate);
                        String printTime = DateUtil.extractTimeFromIso(printTimeStr);
                        model.setFkExchangeTime(printTime);

                        // 航班信息
                        String fkDeparturesDate = DateUtil.extractDateFromIso(fltDate);
                        model.setFkDeparturesDate(fkDeparturesDate);

                        String dptm = map.getString("DCS_DPTM");
                        model.setFkDeparturesTime(StringUtils.isNotBlank(dptm) ? dptm + ":00" : null);

                        model.setFkDepairport(orig);
                        model.setFkArriairport(dest);

                        // 改升后航程：起飞日期 + 承运航司 + 承运航班号 +市场航司+市场航班号 + 起飞机场 + 降落机场
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
                            model.setFkDcSeg(fltDateYmd +fltAirlineCode + fltFlightNumber + mcAirlineCode + mcFlightNumber + orig + dest);
                        }

                        // 乘机人信息
                        String psg_name_en = map.getString("PSG_NAME_EN");
                        String[] result = normalizeName(psg_name_en);
                        model.setEnLastName(result[0]);
                        model.setEnFirstName(result[1]);
                        model.setCnName(map.getString("PSG_NAME_CN"));
                        model.setCertType(map.getString("ID_TYPE"));
                        model.setCertNumber(map.getString("CERT_NO"));

                        // 设置TID
                        model.setFkPassengerUserTid(map.getString("TID"));

                        // 乘机人年龄计算
                        String birthday = map.getString("BIRTHDAY");
                        // 转成yyyy-MM-dd
                        birthday = DateUtil.convertToStandardFormat(birthday);
                        model.setPassengerAge(getAgeByIdCard(model.getCertNumber(), birthday));
                        // 大客户信息
                        String bigCutNum = map.getString("BIG_CUTNUM");
                        model.setAkKeyAccountCode(bigCutNum);
                        model.setKeyAccount(isKeyAccount(bigCutNum));

                        // 舱位信息
                        String sellClass = map.getString("SELL_CLASS");
                        // 换开后舱位
                        model.setAkSegcabin(StringUtils.isNotBlank(sellClass) ? sellClass.substring(0, 1) : null);
                        // 换开后舱等
                        model.setAkCabin(getCabinClass(model.getFkDeparturesDate(), sellClass));


                        // 国内国际标识
                        String ticketType = map.getString("TICKET_TYPE");
                        model.setAkDimark(ticketType);

                        // 航段状态
                        model.setSegmentStatus(map.getString("COUPON_STATUS"));

                        // 常客信息
                        String paxFfp = map.getString("FFP");
                        if (paxFfp != null && paxFfp.length() >= 2) {
                            model.setFfAirline(paxFfp.substring(0, 2));
                            model.setFfrf(SM4Utils.encrypt(paxFfp.substring(2), Constants.SM4_KEY));
                        }
                        model.setFfLevel(map.getString("IS_FFP_JK"));
                        // 大客户号 从机票预订-PNR级反向取值写入此处 和外键串联编号 2.1异步调用查询大客户
                        // 是否为大客户订单

                        // 系统时间
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

        // 3. 写入Doris
        DorisSink<DatechangeSegFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_DATECHANGE_SEG_FACT");
        datechangeSegStream.sinkTo(dorisSink).name("DorisSink-DatechangeSegFact");
    }
}
