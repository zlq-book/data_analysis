package com.travelsky.trp.usercenter.data.analysis.transform.Kfbp;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.trp.utils.DateUtil;
import com.travelsky.dataplatform.utils.DateTimeUtils;
import com.travelsky.dataplatform.utils.DorisUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.RefundSegFactModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class KfbpBusRefundToDwdRefundSegFactTrans {
    private static final Logger logger = LoggerFactory.getLogger(KfbpBusRefundToDwdRefundSegFactTrans.class);
    public static void result(DataStream<JSONObject> source) {
        SingleOutputStreamOperator<RefundSegFactModel> refundSegFactStream = source
                .flatMap(
                        new RichFlatMapFunction<JSONObject, RefundSegFactModel>() {
                            Connection conn;
                            Statement stmt;
                            Map<String, Map<String, String>> exchangeRateMap = new HashMap<>();

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                conn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.ODS_USER, Constants.ODS_PWD);
                                stmt = DorisUtils.getStatement(conn);
                                String exchangeRateQuery = "SELECT " +
                                        "ID, " +
                                        "EXCHANGE_MONTH, " +
                                        "FIVE_DAY_RATE, " +
                                        "CURRENCY_CODE " +
                                        "FROM " + Constants.ODS_DB + ".T_ODS_EXCHANGE_RATE  " +
                                        "ORDER BY EXCHANGE_MONTH DESC, CURRENCY_CODE";
                                ResultSet res = DorisUtils.getDorisResult(stmt, exchangeRateQuery);
                                if (null == res) {
                                    throw new Exception("T_ODS_EXCHANGE_RATE字典数据查询异常");
                                }

                                int count = 0;
                                while (res.next()) {
                                    String month = res.getString("EXCHANGE_MONTH");
                                    String currencyCode = res.getString("CURRENCY_CODE");
                                    String fiveDayRate = res.getString("FIVE_DAY_RATE");

                                    if (month != null && currencyCode != null) {
                                        // 如果该月份还没有对应的币种Map，创建一个新的
                                        if (!exchangeRateMap.containsKey(month)) {
                                            exchangeRateMap.put(month, new HashMap<>());
                                        }
                                        // 将币种和汇率添加到对应月份的Map中
                                        exchangeRateMap.get(month).put(currencyCode, fiveDayRate);
                                        count++;
                                    }
                                }
                            }

                            @Override
                            public void close() throws Exception {
                                DorisUtils.close(conn, stmt, null);
                            }

                            @Override
                            public void flatMap(JSONObject json, Collector<RefundSegFactModel> out) throws Exception {
                                try {
                                    Map<String, String> map = json.toJavaObject(Map.class);

                                    RefundSegFactModel model = new RefundSegFactModel();
                                    // 票号+起飞机场+起飞日期	起飞日期YYYYMMDD格式拼装，票号（不带横杠，纯数字13位）
                                    String ticketNo = map.get("TICKET_NO");
                                    String startCity = map.get("START_CITY");
                                    String fltDepartureDay = map.get("FLT_DEPARTURE_DAY");
                                    if (StringUtils.isBlank(ticketNo) || StringUtils.isBlank(startCity)) {
                                        System.out.println("提取KfbpBusRefundToDwdRefundSegFactTrans失败，主键组成部分不全:" + map.toString());
                                        return;
                                    }

                                    String pkId = ticketNo.replace("-", "") + startCity;
                                    if (StringUtils.isNotBlank(fltDepartureDay)) {
                                        pkId = pkId + DateUtil.formatToYmd(fltDepartureDay);
                                    }
                                    System.out.println("pkId:" + pkId);
                                    model.setPkId(pkId);
                                    model.setAkRefundChannel(map.get("OFFLINE_TICKET_REFUND"));
                                    model.setAkRefundType(map.get("REASON"));
                                    model.setRefundReason(map.get("BUSINESS_TYPE"));
                                    model.setFkRefundUserOriginId(map.get("CUR_TEL"));
                                    String auditingThroughDate = map.get("AUDITING_THROUGH_DATE");
                                    if (StringUtils.isNotBlank(auditingThroughDate)) {
                                        String[] auditingThroughDateStr = auditingThroughDate.split(" ");
                                        if (auditingThroughDateStr.length > 1) {
                                            model.setFkCompleteDate(auditingThroughDateStr[0]);
                                            model.setFkCompleteTime(auditingThroughDateStr[1]);
                                        }
                                    }
                                    // 退订人TID
                                    model.setFkRefundUserTid(map.get("TID"));
                                    String cashCurrencyCode = map.get("CASH_CURRENCY_CODE");
                                    model.setAkCurrency(cashCurrencyCode);

                                    // 原币种应退金额
                                    String refundTotalFee = map.get("REFUND_TOTAL_FEE");
                                    model.setSegCurrefundamount(StringUtils.isNotBlank(refundTotalFee) ? Double.valueOf(refundTotalFee) : null);
                                    // 原币种退票手续费
                                    String recTotalFee = map.get("REC_TOTAL_FEE");
                                    model.setSegCurrefundcharges(StringUtils.isNotBlank(recTotalFee) ? Double.valueOf(recTotalFee) : null);
                                    // 应退金额CNY
                                    if ("CNY".equals(model.getAkCurrency())) {
                                        model.setSegRefundamount(StringUtils.isNotBlank(refundTotalFee) ? Double.valueOf(refundTotalFee) : null);
                                        model.setSegRefundcharges(StringUtils.isNotBlank(recTotalFee) ? Double.valueOf(recTotalFee) : null);
                                    }


                                    model.setSourceLastUpdatetime(map.get("UPDATE_DATE"));

                                    String now = DateTimeUtils.getCurrentDateTime();
                                    model.setSystemCreatetime(now);
                                    model.setSystemLastUpdatetime(now);
                                    out.collect(model);
                                } catch (Exception e) {
                                    //logger.error("处理VIP信息数据失败: {}", json, e);
                                    System.out.println("处理VIP信息数据失败: " + json);
                                }
                            }
                        })
                .returns(TypeInformation.of(RefundSegFactModel.class))
                .name("GdlkDimFactStreamFlatMap");

        // 3. 写入Doris
        DorisSink<RefundSegFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_REFUND_SEG_FACT");
        refundSegFactStream.sinkTo(dorisSink);
    }
    // 查找汇率方法
    //private static Double findExchangeRate(Map<String, Map<String, String>> exchangeRateMap,String currencyCode, String month) {
    //    LocalDate currentDate = LocalDate.parse(month);
    //    String lastMonth = currentDate.minusMonths(1).format(DateTimeFormatter.ofPattern("yyyy-MM"));
    //    lastMonth = lastMonth.replace("-", "");
    //    if (exchangeRateMap.containsKey(lastMonth)) {
    //        Map<String, String> monthRates = exchangeRateMap.get(lastMonth);
    //        if (monthRates.containsKey(currencyCode)) {
    //            String rateStr = monthRates.get(currencyCode);
    //            try {
    //                return Double.parseDouble(rateStr);
    //            } catch (NumberFormatException e) {
    //                System.err.println("汇率数据格式错误: " + rateStr + " for " + currencyCode + " in " + lastMonth);
    //                return null;
    //            }
    //        }
    //    }
    //    return null;
    //}
}
