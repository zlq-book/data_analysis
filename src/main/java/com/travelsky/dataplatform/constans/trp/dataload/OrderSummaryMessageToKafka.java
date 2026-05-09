//package com.travelsky.dataplatform.constans.trp.dataload;
//
//import com.travelsky.dataplatform.constans.trp.utils.Constants;
//import com.travelsky.dataplatform.constans.trp.utils.DateUtil;
//import com.travelsky.dataplatform.constans.trp.utils.HttpClientUtils;
//import com.travelsky.dataplatform.constans.trp.utils.Utils;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.tuple.Tuple4;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//public class OrderSummaryMessageToKafka {
//    /**
//     * 日志实例
//     */
//    public final static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(OrderSpnrMessageToKafka.class);
//    public static ExecutionEnvironment env;
//
//    /**
//     * 将指定日期的数据获取报文后写入hbase
//     *
//     * @param date
//     * @throws Exception
//     */
//    public static void getSummaryMessageToHbase(String date) throws Exception {
//        //获取报文情况记录
//        Tuple4<Long, Long, Long, Long> totalGetXmlStatus = new Tuple4<>(0L, 0L, 0L, 0L);
//
//        //任务开始时间
//        String startTimeSpnrService = DateUtil.formatDate(System.currentTimeMillis(), Constants.RUN_LOG_PATTERN);
//
//        //获取指定日期未获取过SUMMARY报文的snridlist
//        List summaryXmlList = OrderSpnrMessageToKafka.getSpnrIdList(date, Constants.SUMMARY_FLAG);
//        LOGGER.info(date + "需要获取summary报文的spnrID数为" + summaryXmlList.size());
//        if (summaryXmlList.size() > 0) {
//            //将spnrID获取报文并存入hbase   记录获取情况
//            Tuple4<Long, Long, Long, Long> getSummaryXmlStatus = OrderSpnrMessageToKafka.saveXmlToHbase(summaryXmlList, date, Constants.SUMMARY_FLAG, env);
//            totalGetXmlStatus.f2 = getSummaryXmlStatus.f2;
//            totalGetXmlStatus.f3 = getSummaryXmlStatus.f3;
//        } else {
//            HBaseUtils.insertDataLog(date, totalGetXmlStatus.f0, totalGetXmlStatus.f1,
//                    totalGetXmlStatus.f2, totalGetXmlStatus.f3);
//            System.exit(0);
//        }
//
//        String endTimeSpnrService = DateUtil.formatDate(System.currentTimeMillis(), Constants.RUN_LOG_PATTERN);
//        HBaseUtils.insertRunLog(startTimeSpnrService, endTimeSpnrService, totalGetXmlStatus.f1,
//                totalGetXmlStatus.f0, Constants.SPNR_SERVICE, date,
//                Utils.getHbaseTbaleName(Constants.HBASE_TABLE_ORDER_SPNR, date));
//
//        String startTimeSummaryService = DateUtil.formatDate(System.currentTimeMillis(), Constants.RUN_LOG_PATTERN);
//
//        //判断报文是否正常获取并写入hbase表  来更新fetch_log表状态
//        ckeckGetXmlWhetherSucceed(summaryXmlList, date);
//        //任务结束时间
//        String endTimeSummaryService = DateUtil.formatDate(System.currentTimeMillis(), Constants.RUN_LOG_PATTERN);
//        //记录日志
//        HBaseUtils.insertRunLog(startTimeSummaryService, endTimeSummaryService, totalGetXmlStatus.f3,
//                totalGetXmlStatus.f2, Constants.SUMMARY_SERVICE, date,
//                Utils.getHbaseTbaleName(Constants.HBASE_TABLE_ORDER_SPNR, date));
//
//        // 记录迁移数据统计表
//        HBaseUtils.insertDataLog(date, totalGetXmlStatus.f0, totalGetXmlStatus.f1,
//                totalGetXmlStatus.f2, totalGetXmlStatus.f3);
//
//    }
//
//    /**
//     * 判断spnrID是否正常获取到spnr报文和summary报文
//     * 成功获取时 更新fetch_log表对应状态
//     *
//     * @param summaryXmlList
//     */
//    public static void ckeckGetXmlWhetherSucceed(List<String> summaryXmlList, String date) throws IOException {
//        LOGGER.info("开始更新fetch_log表spnrID获取报文状态");
//        for (String spnrID : summaryXmlList) {
//            Map<String, String> map = HBaseUtils.searchByRowKey(Utils.getHbaseTbaleName(Constants.HBASE_TABLE_ORDER_SPNR, date), spnrID);
//            if (StringUtils.isNotBlank(map.get(Constants.SUMMARY_XML))) {
//                HBaseUtils.insertIndex(Constants.HBASE_TABLE_ORDER_FETCH_LOG, spnrID, Constants.SUMMARY_FLAG, Constants.GET_XML_SUCCESS_MARK, false);
//            } else {
//                HBaseUtils.insertIndex(Constants.HBASE_TABLE_ORDER_FETCH_LOG, spnrID, Constants.SUMMARY_FLAG, Constants.GET_XML_FAIL_MARK, false);
//            }
//        }
//        LOGGER.info("fetch_log表获取报文状态更新完成");
//
//    }
//
//}
