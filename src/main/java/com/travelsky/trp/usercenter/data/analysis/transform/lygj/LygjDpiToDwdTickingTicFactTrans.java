package com.travelsky.trp.usercenter.data.analysis.transform.lygj;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.trp.utils.DateUtil;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingTicFactModel;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Objects;

import static com.travelsky.dataplatform.utils.FieldType.DOCUMENT_TYPE;
import static com.travelsky.dataplatform.utils.NormalizationUtils.standardize;
import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.isKeyAccount;
import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.normalizeName;

/**
 * * 机票出票-客票级
 */
public class LygjDpiToDwdTickingTicFactTrans {
    //private static final Logger logger = LoggerFactory.getLogger(LygjDpiToDwdTickingTicFactTrans.class);

    public static void result(DataStream<JSONObject> source) {
        DataStream<TickingTicFactModel> tickingTicStream = source
                .filter(map -> StringUtils.isNotBlank(map.getString("ET_NUM")))
                .map(map -> {
                    TickingTicFactModel model = new TickingTicFactModel();
                    String printTime = map.getString("PRINT_TICKET_TIME");
                    String printDate = DateUtil.formatToYmd(printTime);
                    String etNum = map.getString("ET_NUM");
                    if (StringUtils.isBlank(etNum)  || StringUtils.isBlank(printTime)) {
                        System.out.println("提取LygjDpiToDwdTickingTicFactTrans失败，票号为空: " + map.toString());
                        //logger.error("提取LygjDpiToDwdTickingTicFactTrans失败，主键组成部分为空: {}", map.toString());
                        return null;
                    } else {
                        model.setPkId(printDate + etNum);
                        // 出票信息
                        String pnr = map.getString("PNR_ICS");
                        // 出票类型：ORIGINALTKNE非空为换开出票，否则为普通出票
                        model.setAkTicketType(map.getString("ORIGINALTKNE") != null ? "ReIssue" : "Issue");
                        // PNR编号
                        model.setPnrNumber(pnr);
                        // 票号
                        model.setTicketNumber(etNum);
                        // 出票office号
                        model.setAkBookingOfficeNumber(map.getString("ISSUE_OFFI"));
                        // 出票日期和时间
                        model.setFkTicketingDate(DateUtil.extractDateFromIso(printTime));
                        model.setFkTicketingTime(DateUtil.extractTimeFromIso(printTime));

                        // 乘机人信息
                        String psg_name_en = map.getString("PSG_NAME_EN");
                        String[] result = normalizeName(psg_name_en);
                        model.setEnLastName(result[0]);
                        model.setEnFirstName(result[1]);
                        model.setCnName(map.getString("PSG_NAME_CN"));
                        model.setCertType(standardize(DOCUMENT_TYPE, map.getString("ID_TYPE"), DataSource.LUYAN_STEWARD));
                        model.setCertNumber(map.getString("CERT_NO"));

                        // 设置TID
                        model.setFkPassengerUserTid(map.getString("TID"));

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
                        // 常客信息
                        String ffp = map.getString("FFP");
                        if (ffp != null && ffp.length() >= 2) {
                            model.setFfAirline(ffp.substring(0, 2));
                            model.setFfrf(SM4Utils.encrypt(ffp.substring(2), Constants.SM4_KEY));
                        }
                        model.setFfLevel(map.getString("IS_FFP_JK"));

                        // 其他属性
                        String ticketType = map.getString("TICKET_TYPE");
                        model.setAkDimark(ticketType);
                        model.setAkConjuction("1".equals(map.getString("IO_FLAG")));
                        model.setAkFarebasis(map.getString("FARE_BASIS_CODE"));
                        model.setUnaccompaniedMinor("1".equals(map.getString("IS_UM")));
                        model.setGovernmentPurchase("1".equals(map.getString("TKNEGP")));
                        // 是否是团队票
                        model.setAkTeammark("Y".equals(map.getString("IS_GROUP")));
                        // 大客户信息
                        String bigCutNum = map.getString("BIG_CUTNUM");
                        model.setAkKeyAccountCode(bigCutNum);
                        model.setKeyAccount(isKeyAccount(bigCutNum));

                        // 同行人数
                        String passengerNumber = map.getString("PASSENGER_NUMBER");
                        int peerNumber = passengerNumber != null ? Integer.parseInt(passengerNumber) : 1;
                        model.setAkPeerNumber(peerNumber);
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

                        /*提前出票天数（客票级）"机票出票-客票级
                        出票-航段级搜索。通过票号去机票-出票-航段级搜索。
                        按照起飞日期排序。
                        第一段航班起飞日期减去预订日期+
                        第2段航班起飞日期减去预订日期+
                        第N段航班起飞日期减去预订日期，向上取整到个位"*/
                        //Connection conn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
                        //Statement stmt = DorisUtils.getStatement(conn);
                        //String sql = "SELECT FK_PASSENGER_USER_TID FROM " + Constants.DWD_DB + ".T_DWD_TICKING_SEG_FACT WHERE FK_PASSENGER_USER_TID = '" + map.getString("TID") + "'";
                        //ResultSet rs = DorisUtils.getDorisResult(stmt, sql);
                        //if (rs.next()) {
                        //    //model.setfir();
                        //}
                        //DorisUtils.close(conn, stmt, rs);

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
        DorisSink<TickingTicFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_TICKING_TIC_FACT");
        tickingTicStream.sinkTo(dorisSink).name("DorisSink-TickingTicFact");
    }
}
