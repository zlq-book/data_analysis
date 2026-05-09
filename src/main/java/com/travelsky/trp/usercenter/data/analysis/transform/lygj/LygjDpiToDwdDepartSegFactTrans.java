package com.travelsky.trp.usercenter.data.analysis.transform.lygj;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.trp.utils.DateUtil;
import com.travelsky.dataplatform.utils.DateTimeUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.DepartSegFactModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;

import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.*;

/**
 * 鲁雁管家明细事实表-成行-航段
 * COUPON_STATUS=F的数据，进入本表
 */
public class LygjDpiToDwdDepartSegFactTrans {
    // 日志记录器
    //static final Logger logger = LoggerFactory.getLogger(LygjDpiToDwdDepartSegFactTrans.class);

    /**
     * 数据流处理主方法
     */
    public static void result(DataStream<JSONObject> source) {
        // 新增过滤 成型表是AC状态 过滤非AC过数据
        DataStream<DepartSegFactModel> departSegStream = source
                .filter(map -> "AC".equalsIgnoreCase(map.get("STATUS").toString()))
                .map(map -> {
                    DepartSegFactModel model = new DepartSegFactModel();
                    String fltDate = map.getString("FLT_DATE");
                    String etNum = map.getString("ET_NUM");
                    String orig = map.getString("ORIG");
                    String dest = map.getString("DEST");

                    if (StringUtils.isBlank(fltDate) || StringUtils.isBlank(etNum) || StringUtils.isBlank(orig) || StringUtils.isBlank(dest)) {
                        System.out.println("提取LygjDpiToDwdDepartSegFactTrans失败，主键部分组成为空: " + map.toString());
                        //logger.error("提取LygjDpiToDwdDepartSegFactTrans失败，主键部分组成为空: {}", map.toString());
                        return null;
                    } else {
                        String fltDateYmd = DateUtil.formatToYmd(fltDate);
                        // 票号+起飞机场+起飞日期
                        model.setPkId(etNum + orig + fltDateYmd);

                        // 票号
                        model.setTikNum(etNum);

                        // 航班日期
                        String fkDeparturesDate = DateUtil.extractDateFromIso(fltDate);
                        model.setFkDeparturesDate(fkDeparturesDate);

                        // 到达日期
                        String atDateStr = map.getString("DCS_ATDATE");
                        String atDate = DateUtil.extractDateFromIso(atDateStr);
                        model.setFkArriveDate(atDate);

                        // 航班时间
                        String dptm = map.getString("DCS_DPTM");
                        model.setFkDeparturesTime(StringUtils.isNotBlank(dptm) ? dptm + ":00" : null);
                        String attm = map.getString("DCS_ATTM");
                        model.setFkArriveTime(StringUtils.isNotBlank(attm) ? attm + ":00" : null);

                        // 航程构建 起飞日期 + 承运航司 + 承运航班号 +市场航司+市场航班号+ 起飞机场 + 降落机场
                        String mcFlt = map.getString("MC_FLT");
                        String mcAirlineCode = (mcFlt != null && mcFlt.length() >= 2) ? mcFlt.substring(0, 2) : null;
                        String mcFlightNumber = (mcFlt != null && mcFlt.length() > 2) ? mcFlt.substring(2) : null;
                        String fltFlt = map.getString("FLT_NUM");
                        String fltAirlineCode = (fltFlt != null && fltFlt.length() >= 2) ? fltFlt.substring(0, 2) : null;
                        String fltFlightNumber = (fltFlt != null && fltFlt.length() > 2) ? fltFlt.substring(2) : null;
                        if (StringUtils.isNotBlank(fltDateYmd) && StringUtils.isNotBlank(fltAirlineCode)
                                && StringUtils.isNotBlank(fltFlightNumber) && StringUtils.isNotBlank(mcAirlineCode)
                                && StringUtils.isNotBlank(mcFlightNumber) && StringUtils.isNotBlank(orig) && StringUtils.isNotBlank(dest)) {
                            model.setFkPnrdSeg(fltDateYmd +fltAirlineCode + fltFlightNumber + mcAirlineCode + mcFlightNumber + orig + dest);
                        }
                        // 起飞机场
                        model.setFkDepairport(orig);
                        // 到达机场
                        model.setFkArriairport(dest);

                        // 设置TID
                        model.setFkPassengerUserTid(map.getString("TID"));
                        // 是否是团队票
                        model.setAkTeammark("Y".equals(map.getString("IS_GROUP")));
                        // 乘机人信息
                        model.setCnName(map.getString("PSG_NAME_CN"));
                        String psg_name_en = map.getString("PSG_NAME_EN");
                        String[] result = normalizeName(psg_name_en);
                        model.setEnLastName(result[0]);
                        model.setEnFirstName(result[1]);

                        model.setCertType(map.getString("ID_TYPE"));
                        model.setCertNumber(map.getString("CERT_NO"));

                        // 大客户信息
                        String bigCutNum = map.getString("BIG_CUTNUM");
                        model.setKeyAccount(isKeyAccount(bigCutNum));

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
                        String sellClass = map.getString("DCS_SELL_CLASS");
                        model.setAkSegcabin(StringUtils.isNotBlank(sellClass) ? sellClass.substring(0, 1) : null);
                        model.setAkCabin(getCabinClass(model.getFkDeparturesDate(), sellClass));

                        // 国内国际标识
                        String ticketType = map.getString("TICKET_TYPE");
                        model.setAkDimark( ticketType);
                        model.setPassengerType(map.getString("USER_TYPE"));

                        //是否与儿童同行
                        model.setPeerChd("CHD".equals(map.getString("USER_TYPE")));
                        // 是否与老人同行
                        if (null != model.getPassengerAge() && model.getPassengerAge() > 65) {
                            model.setPeerSenior(true);
                        } else {
                            model.setPeerSenior(false);
                        }

                        // 提前出票天数
                        //model.setAkAdvbookDay(getAdvBookDay(map.getString("DCS_ADV_BOOK_DATE")));

                        // 是否为大客户订单
                        //model.setKeyAccount();

                        // V2新增
                        // 出票信息
                        String printTicketTimeStr = map.getString("PRINT_TICKET_TIME");
                        String printTicketDate = DateUtil.extractDateFromIso(printTicketTimeStr);
                        String printTicketTime = DateUtil.extractTimeFromIso(printTicketTimeStr);
                        model.setFkIssueDate(printTicketDate);
                        model.setFkIssueTime(printTicketTime);

                        // 系统时间
                        String now = DateTimeUtils.getCurrentDateTime();
                        model.setSystemCreatetime(now);

                        model.setDataActive(true);
                        model.setDataActiveTime(now);
                        return model;
                    }
                }).setParallelism(4)
                .filter(model -> model != null);

        // 写入Doris
        DorisSink<DepartSegFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_DEPART_SEG_FACT");
        departSegStream.sinkTo(dorisSink).name("DorisSink-DepartSegFact");
    }
}
